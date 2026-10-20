//! HTTP/1.1 message framing for the `io_uring` server: request heads parsed
//! with `httparse` (header values point into the receive buffer rather than
//! being copied), body framing per RFC 9112, and response heads.

use std::cell::RefCell;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::header::{
    CONNECTION, CONTENT_LENGTH, DATE, EXPECT, HeaderName, HeaderValue, TRANSFER_ENCODING,
};
use axum::http::{HeaderMap, Method, Request, StatusCode, Uri, Version};
use bytes::{BufMut, Bytes, BytesMut};
use http_body::{Body, Frame, SizeHint};
use tokio::sync::mpsc;

/// Largest request head accepted.
pub const MAX_HEAD: usize = 64 * 1024;
const MAX_HEADERS: usize = 100;

/// How a request body is delimited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    Empty,
    Length(u64),
    Chunked,
}

/// A parsed request head.
pub struct Head {
    pub request: Request<()>,
    pub framing: Framing,
    /// The client allows the connection to be reused after this exchange.
    pub keep_alive: bool,
    /// The client waits for `100 Continue` before sending the body.
    pub expect_continue: bool,
}

/// A request that can't be served; answered with `status` and a close.
#[derive(Debug)]
pub struct Rejection(pub StatusCode);

/// Parses a request head from the front of `buf`, removing it. `Ok(None)`
/// means more bytes are needed.
///
/// # Errors
///
/// A [`Rejection`] for a malformed, oversized or unsupported head.
pub fn parse_head(buf: &mut BytesMut) -> Result<Option<Head>, Rejection> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut parsed = httparse::Request::new(&mut headers);
    let len = match parsed.parse(buf) {
        Ok(httparse::Status::Complete(len)) => len,
        Ok(httparse::Status::Partial) if buf.len() >= MAX_HEAD => {
            return Err(Rejection(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE));
        }
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(httparse::Error::TooManyHeaders) => {
            return Err(Rejection(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE));
        }
        Err(_) => return Err(Rejection(StatusCode::BAD_REQUEST)),
    };
    let bad = || Rejection(StatusCode::BAD_REQUEST);

    // Values are kept as offsets so they can point into the frozen head.
    let base = buf.as_ptr() as usize;
    let offset = |slice: &[u8]| (slice.as_ptr() as usize - base, slice.len());
    let method =
        Method::from_bytes(parsed.method.ok_or_else(bad)?.as_bytes()).map_err(|_| bad())?;
    let path = offset(parsed.path.ok_or_else(bad)?.as_bytes());
    let version = match parsed.version {
        Some(1) => Version::HTTP_11,
        Some(0) => Version::HTTP_10,
        _ => return Err(bad()),
    };
    let mut fields = Vec::with_capacity(parsed.headers.len());
    for header in parsed.headers.iter() {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| bad())?;
        fields.push((name, offset(header.value)));
    }

    let head = buf.split_to(len).freeze();
    let slice = |(start, len): (usize, usize)| head.slice(start..start + len);
    let uri = Uri::from_maybe_shared(slice(path)).map_err(|_| bad())?;
    let mut map = HeaderMap::with_capacity(fields.len());
    for (name, value) in fields {
        let value = HeaderValue::from_maybe_shared(slice(value)).map_err(|_| bad())?;
        map.append(name, value);
    }

    let framing = framing(&map, version)?;
    let connection_has = |token: &str| {
        map.get_all(CONNECTION).iter().any(|value| {
            value.to_str().is_ok_and(|value| {
                value
                    .split(',')
                    .any(|item| item.trim().eq_ignore_ascii_case(token))
            })
        })
    };
    let keep_alive = match version {
        Version::HTTP_11 => !connection_has("close"),
        _ => connection_has("keep-alive"),
    }
    // A body framed both ways may be a smuggling attempt: serve it, then close.
    && !(map.contains_key(TRANSFER_ENCODING) && map.contains_key(CONTENT_LENGTH));
    let expect_continue = version == Version::HTTP_11
        && map
            .get(EXPECT)
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"100-continue"));

    let mut request = Request::new(());
    *request.method_mut() = method;
    *request.uri_mut() = uri;
    *request.version_mut() = version;
    *request.headers_mut() = map;
    Ok(Some(Head {
        request,
        framing,
        keep_alive,
        expect_continue,
    }))
}

fn framing(headers: &HeaderMap, version: Version) -> Result<Framing, Rejection> {
    if headers.contains_key(TRANSFER_ENCODING) {
        if version == Version::HTTP_10 {
            return Err(Rejection(StatusCode::BAD_REQUEST));
        }
        // Chunked must be the final coding; other codings aren't supported.
        let mut codings = headers
            .get_all(TRANSFER_ENCODING)
            .iter()
            .flat_map(|value| value.to_str().unwrap_or("?").split(','))
            .map(str::trim)
            .filter(|coding| !coding.is_empty());
        return match (codings.next(), codings.next()) {
            (Some(coding), None) if coding.eq_ignore_ascii_case("chunked") => Ok(Framing::Chunked),
            _ => Err(Rejection(StatusCode::NOT_IMPLEMENTED)),
        };
    }
    let mut length = None;
    for value in headers.get_all(CONTENT_LENGTH) {
        for item in value
            .to_str()
            .map_err(|_| Rejection(StatusCode::BAD_REQUEST))?
            .split(',')
        {
            let item = item.trim();
            if item.is_empty() || !item.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(Rejection(StatusCode::BAD_REQUEST));
            }
            let parsed: u64 = item
                .parse()
                .map_err(|_| Rejection(StatusCode::BAD_REQUEST))?;
            if length.is_some_and(|length| length != parsed) {
                return Err(Rejection(StatusCode::BAD_REQUEST));
            }
            length = Some(parsed);
        }
    }
    Ok(match length {
        None | Some(0) => Framing::Empty,
        Some(length) => Framing::Length(length),
    })
}

/// Parses a chunk-size line (without its CRLF), ignoring extensions.
///
/// # Errors
///
/// If the line isn't a hexadecimal size.
pub fn chunk_size(line: &[u8]) -> io::Result<u64> {
    let size = line.split(|&byte| byte == b';').next().unwrap_or_default();
    let size = std::str::from_utf8(size)
        .map_err(|_| invalid_chunk())?
        .trim_matches(|c| c == ' ' || c == '\t');
    if size.is_empty() || size.len() > 16 {
        return Err(invalid_chunk());
    }
    u64::from_str_radix(size, 16).map_err(|_| invalid_chunk())
}

fn invalid_chunk() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid chunked request body")
}

/// A request body fed by the connection as it receives it.
pub struct ChannelBody {
    frames: mpsc::Receiver<io::Result<Bytes>>,
    /// Bytes still to come, when the length is known.
    remaining: Option<u64>,
    done: bool,
}

impl ChannelBody {
    #[must_use]
    pub fn new(frames: mpsc::Receiver<io::Result<Bytes>>, length: Option<u64>) -> Self {
        Self {
            frames,
            remaining: length,
            done: false,
        }
    }
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.frames.poll_recv(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(remaining) = &mut self.remaining {
                    *remaining = remaining.saturating_sub(frame.len() as u64);
                }
                Poll::Ready(Some(Ok(Frame::data(frame))))
            }
            Poll::Ready(Some(Err(err))) => {
                self.done = true;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                self.done = true;
                // The connection only stops feeding early after sending an error.
                if self.remaining.is_some_and(|remaining| remaining > 0) {
                    return Poll::Ready(Some(Err(io::ErrorKind::UnexpectedEof.into())));
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done || self.remaining == Some(0)
    }

    fn size_hint(&self) -> SizeHint {
        self.remaining
            .map_or_else(SizeHint::default, SizeHint::with_exact)
    }
}

/// How a response body is delimited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseFraming {
    /// No body at all (HEAD, 1xx, 204, 304).
    None,
    Length(u64),
    Chunked,
    /// Ends when the connection closes (HTTP/1.0 clients, unknown length).
    Close,
}

/// Decides the response framing and adds the headers that go with it.
pub fn response_framing(
    status: StatusCode,
    headers: &mut HeaderMap,
    size_hint: &SizeHint,
    head_request: bool,
    version: Version,
    keep_alive: &mut bool,
) -> ResponseFraming {
    let bodiless_status = status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED;
    if bodiless_status {
        headers.remove(TRANSFER_ENCODING);
        if status != StatusCode::NOT_MODIFIED {
            headers.remove(CONTENT_LENGTH);
        }
        return ResponseFraming::None;
    }
    let declared = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    // Framing is this server's business, not the handler's.
    headers.remove(TRANSFER_ENCODING);
    let framing = if let Some(length) = declared {
        ResponseFraming::Length(length)
    } else if let Some(length) = size_hint.exact() {
        headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
        ResponseFraming::Length(length)
    } else if head_request {
        ResponseFraming::None
    } else if version == Version::HTTP_11 {
        headers.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
        ResponseFraming::Chunked
    } else {
        *keep_alive = false;
        ResponseFraming::Close
    };
    if head_request {
        ResponseFraming::None
    } else {
        framing
    }
}

/// Serializes a response head.
pub fn encode_head(
    status: StatusCode,
    headers: &mut HeaderMap,
    version: Version,
    keep_alive: bool,
) -> Bytes {
    if !keep_alive {
        headers.insert(CONNECTION, HeaderValue::from_static("close"));
    } else if version == Version::HTTP_10 {
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    }
    if !headers.contains_key(DATE) {
        headers.insert(DATE, date());
    }
    let mut head = BytesMut::with_capacity(
        64 + headers
            .iter()
            .map(|(name, value)| name.as_str().len() + value.len() + 4)
            .sum::<usize>(),
    );
    head.put_slice(b"HTTP/1.1 ");
    head.put_slice(status.as_str().as_bytes());
    head.put_u8(b' ');
    head.put_slice(status.canonical_reason().unwrap_or("Unknown").as_bytes());
    head.put_slice(b"\r\n");
    for (name, value) in headers.iter() {
        head.put_slice(name.as_str().as_bytes());
        head.put_slice(b": ");
        head.put_slice(value.as_bytes());
        head.put_slice(b"\r\n");
    }
    head.put_slice(b"\r\n");
    head.freeze()
}

/// The `Date` header value, formatted once per second per thread.
fn date() -> HeaderValue {
    thread_local! {
        static CACHED: RefCell<(u64, Option<HeaderValue>)> = const { RefCell::new((0, None)) };
    }
    let now = SystemTime::now();
    let second = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    CACHED.with(|cached| {
        let mut cached = cached.borrow_mut();
        if cached.0 != second || cached.1.is_none() {
            *cached = (
                second,
                HeaderValue::from_str(&httpdate::fmt_http_date(now)).ok(),
            );
        }
        cached
            .1
            .clone()
            .unwrap_or_else(|| HeaderValue::from_static(""))
    })
}

/// A minimal response for requests rejected before reaching the router.
#[must_use]
pub fn rejection_response(status: StatusCode) -> Bytes {
    Bytes::from(format!(
        "HTTP/1.1 {} {}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        status.as_str(),
        status.canonical_reason().unwrap_or("Error"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> Result<Option<Head>, Rejection> {
        parse_head(&mut BytesMut::from(raw))
    }

    #[test]
    fn parses_heads() {
        let mut buf = BytesMut::from(
            "PUT /upload/1?comp=block HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nX-A: 1\r\nx-a: 2\r\n\r\nhello",
        );
        let head = parse_head(&mut buf).unwrap().unwrap();
        assert_eq!(&buf[..], b"hello");
        assert_eq!(head.request.method(), Method::PUT);
        assert_eq!(head.request.uri().path(), "/upload/1");
        assert_eq!(head.request.uri().query(), Some("comp=block"));
        assert_eq!(head.framing, Framing::Length(5));
        assert!(head.keep_alive);
        assert_eq!(head.request.headers().get_all("x-a").iter().count(), 2);

        assert!(parse("GET / HTTP/1.1\r\nHost: x\r\n").unwrap().is_none());
        let head = parse("GET / HTTP/1.0\r\n\r\n").unwrap().unwrap();
        assert!(!head.keep_alive);
        assert_eq!(head.framing, Framing::Empty);
        let head = parse("GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n")
            .unwrap()
            .unwrap();
        assert!(head.keep_alive);
        let head = parse("GET / HTTP/1.1\r\nConnection: close\r\n\r\n")
            .unwrap()
            .unwrap();
        assert!(!head.keep_alive);
        let head =
            parse("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n")
                .unwrap()
                .unwrap();
        assert_eq!(head.framing, Framing::Chunked);
        assert!(head.expect_continue);
    }

    #[test]
    fn rejects_bad_framing() {
        for raw in [
            "POST / HTTP/1.1\r\nContent-Length: 1, 2\r\n\r\n",
            "POST / HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
            "POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
            "POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n",
            "GARBAGE\r\n\r\n",
        ] {
            assert_eq!(
                parse(raw).err().map(|r| r.0),
                Some(StatusCode::BAD_REQUEST),
                "{raw}"
            );
        }
        assert_eq!(
            parse("POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n")
                .err()
                .map(|r| r.0),
            Some(StatusCode::NOT_IMPLEMENTED)
        );
        let huge = format!("GET / HTTP/1.1\r\nX: {}", "a".repeat(MAX_HEAD));
        assert_eq!(
            parse(&huge).err().map(|r| r.0),
            Some(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE)
        );
        // Both framings: the length is ignored and the connection closed after.
        let head =
            parse("POST / HTTP/1.1\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n")
                .unwrap()
                .unwrap();
        assert_eq!(head.framing, Framing::Chunked);
        assert!(!head.keep_alive);
    }

    #[test]
    fn parses_chunk_sizes() {
        assert_eq!(chunk_size(b"1a").unwrap(), 26);
        assert_eq!(chunk_size(b"FF;ext=1").unwrap(), 255);
        assert_eq!(chunk_size(b"0").unwrap(), 0);
        assert!(chunk_size(b"").is_err());
        assert!(chunk_size(b"xyz").is_err());
        assert!(chunk_size(b"11111111111111111").is_err());
    }

    #[test]
    fn frames_responses() {
        let mut keep_alive = true;
        let mut headers = HeaderMap::new();
        let framing = response_framing(
            StatusCode::OK,
            &mut headers,
            &SizeHint::with_exact(3),
            false,
            Version::HTTP_11,
            &mut keep_alive,
        );
        assert_eq!(framing, ResponseFraming::Length(3));
        assert_eq!(headers[CONTENT_LENGTH], "3");

        let mut headers = HeaderMap::new();
        let framing = response_framing(
            StatusCode::OK,
            &mut headers,
            &SizeHint::default(),
            false,
            Version::HTTP_11,
            &mut keep_alive,
        );
        assert_eq!(framing, ResponseFraming::Chunked);
        assert!(keep_alive);

        let mut headers = HeaderMap::new();
        let framing = response_framing(
            StatusCode::OK,
            &mut headers,
            &SizeHint::default(),
            false,
            Version::HTTP_10,
            &mut keep_alive,
        );
        assert_eq!(framing, ResponseFraming::Close);
        assert!(!keep_alive);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from(10));
        let framing = response_framing(
            StatusCode::OK,
            &mut headers,
            &SizeHint::default(),
            true,
            Version::HTTP_11,
            &mut true,
        );
        assert_eq!(framing, ResponseFraming::None);
        assert_eq!(headers[CONTENT_LENGTH], "10");

        let head = encode_head(
            StatusCode::CREATED,
            &mut HeaderMap::new(),
            Version::HTTP_11,
            false,
        );
        let head = std::str::from_utf8(&head).unwrap();
        assert!(head.starts_with("HTTP/1.1 201 Created\r\n"));
        assert!(head.contains("connection: close\r\n"));
        assert!(head.contains("date: "));
        assert!(head.ends_with("\r\n\r\n"));
    }
}
