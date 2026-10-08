# syntax=docker/dockerfile:1

# The builder always runs on the build host and cross-compiles for the target
# platform, so multi-platform images don't compile Rust under QEMU emulation.
FROM --platform=$BUILDPLATFORM rust:1.97-bookworm AS builder

ARG BUILDARCH
ARG TARGETARCH
RUN case "$TARGETARCH" in \
      amd64) echo x86_64-unknown-linux-gnu > /rust-target ;; \
      arm64) echo aarch64-unknown-linux-gnu > /rust-target ;; \
      *) echo "Unsupported target architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && rustup target add "$(cat /rust-target)" \
    && if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      case "$TARGETARCH" in \
        amd64) toolchain=gcc-x86-64-linux-gnu ;; \
        arm64) toolchain=gcc-aarch64-linux-gnu ;; \
      esac; \
      apt-get update \
      && apt-get install -y --no-install-recommends "$toolchain" "libc6-dev-$TARGETARCH-cross" \
      && rm -rf /var/lib/apt/lists/*; \
    fi

# Linkers and C compilers (for `ring`) per target; the native one already exists.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

ARG BUILD_HASH
ENV BUILD_HASH=${BUILD_HASH}
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    target="$(cat /rust-target)" \
    && cargo build --release --locked --target "$target" \
    && cp "target/$target/release/github-actions-cache-server" /usr/local/bin/

# --------------------------------------------

# glibc, CA certificates and nothing else.
FROM gcr.io/distroless/cc-debian12 AS runner

COPY --from=builder /usr/local/bin/github-actions-cache-server /usr/local/bin/github-actions-cache-server

ENV PORT=3000
EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/github-actions-cache-server"]
