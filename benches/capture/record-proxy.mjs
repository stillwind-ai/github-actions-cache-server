#!/usr/bin/env node
// Recording reverse proxy for capturing cache traffic from real clients.
//
//   node record-proxy.mjs <listen port> <upstream base URL> <trace.jsonl>
//
// Point the cache server's API_BASE_URL (and the client's ACTIONS_RESULTS_URL)
// at this proxy so uploads and downloads flow through it too. Every request
// becomes one JSON line: connection, timing, method, URL, the headers that
// shape traffic, byte counts and status. Twirp JSON bodies (keys, versions,
// restore keys) are kept whole; payload bytes are only counted.
import http from 'node:http'
import fs from 'node:fs'

const [port, upstreamBase, tracePath] = process.argv.slice(2)
if (!port || !upstreamBase || !tracePath) {
  console.error('usage: record-proxy.mjs <port> <upstream base URL> <trace.jsonl>')
  process.exit(2)
}
const upstream = new URL(upstreamBase)
const trace = fs.createWriteStream(tracePath, { flags: 'a' })
const started = process.hrtime.bigint()
const now = () => Number(process.hrtime.bigint() - started) / 1e6
const REQUEST_HEADERS = ['content-length', 'content-type', 'range', 'x-ms-range', 'x-ms-blob-type',
  'x-ms-version', 'x-ms-range-get-content-md5', 'user-agent', 'transfer-encoding', 'accept-encoding',
  'connection', 'expect']
const RESPONSE_HEADERS = ['content-length', 'content-range', 'accept-ranges', 'content-type',
  'transfer-encoding', 'connection']
const pick = (headers, names) =>
  Object.fromEntries(names.filter((name) => headers[name] !== undefined).map((name) => [name, headers[name]]))
const keepBody = (path, contentType) => path.includes('/twirp/') || (contentType ?? '').includes('json') || (contentType ?? '').includes('xml')
const MAX_KEPT = 64 * 1024

let nextConnection = 0
let nextRequest = 0
let active = 0
const agent = new http.Agent({ keepAlive: true, maxSockets: Infinity })

const server = http.createServer((req, res) => {
  const record = {
    id: nextRequest++,
    conn: req.socket.connId,
    t_start: now(),
    active_at_start: ++active,
    method: req.method,
    url: req.url,
    http: req.httpVersion,
    req_headers: pick(req.headers, REQUEST_HEADERS),
    req_bytes: 0,
  }
  const reqKeep = keepBody(req.url, req.headers['content-type'])
  const reqChunks = []
  const proxied = http.request({
    host: upstream.hostname,
    port: upstream.port,
    method: req.method,
    path: req.url,
    headers: { ...req.headers, host: upstream.host },
    agent,
  }, (upstreamRes) => {
    record.status = upstreamRes.statusCode
    record.resp_headers = pick(upstreamRes.headers, RESPONSE_HEADERS)
    record.t_headers = now()
    record.resp_bytes = 0
    const respKeep = keepBody(req.url, upstreamRes.headers['content-type'])
    const respChunks = []
    res.writeHead(upstreamRes.statusCode, upstreamRes.headers)
    upstreamRes.on('data', (chunk) => {
      record.resp_bytes += chunk.length
      if (respKeep && record.resp_bytes <= MAX_KEPT) respChunks.push(chunk)
    })
    upstreamRes.pipe(res)
    const finish = (how) => {
      if (record.t_end !== undefined) return
      record.t_end = now()
      record.end = how
      active--
      if (respKeep) record.resp_body = Buffer.concat(respChunks).toString('utf8')
      trace.write(JSON.stringify(record) + '\n')
    }
    res.on('finish', () => finish('ok'))
    res.on('close', () => finish(res.writableFinished ? 'ok' : 'client-aborted'))
  })
  proxied.on('error', (err) => {
    record.error = String(err)
    record.t_end = now()
    active--
    trace.write(JSON.stringify(record) + '\n')
    if (!res.headersSent) res.writeHead(502)
    res.end()
  })
  req.on('data', (chunk) => {
    record.req_bytes += chunk.length
    if (reqKeep && record.req_bytes <= MAX_KEPT) reqChunks.push(chunk)
  })
  req.on('end', () => {
    record.t_req_end = now()
    if (reqKeep) record.req_body = Buffer.concat(reqChunks).toString('utf8')
  })
  req.pipe(proxied)
})
server.on('connection', (socket) => {
  socket.connId = nextConnection++
})
server.keepAliveTimeout = 65_000
server.requestTimeout = 0
server.listen(Number(port), '127.0.0.1', () => {
  console.error(`recording ${upstream.href} on http://127.0.0.1:${port} -> ${tracePath}`)
})
