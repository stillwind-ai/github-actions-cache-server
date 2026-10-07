FROM rust:1.97-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

ARG BUILD_HASH
ENV BUILD_HASH=${BUILD_HASH}
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
    && cp target/release/github-actions-cache-server /usr/local/bin/

# --------------------------------------------

# glibc, CA certificates and nothing else.
FROM gcr.io/distroless/cc-debian12 AS runner

COPY --from=builder /usr/local/bin/github-actions-cache-server /usr/local/bin/github-actions-cache-server

ENV PORT=3000
EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/github-actions-cache-server"]
