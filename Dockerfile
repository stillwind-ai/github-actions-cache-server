# syntax=docker/dockerfile:1

# The Rust stages always run on the build host and cross-compile for the target
# platform, so multi-platform images don't compile Rust under QEMU emulation.
#
# cargo-chef splits the build in two: dependencies are compiled in a layer that
# only depends on Cargo.toml and Cargo.lock, so a source-only change reuses it.
# Unlike a cache mount, that layer is exported to the registry/GHA build cache.
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:0.1.78-rust-1.97-bookworm AS chef
WORKDIR /app

# --------------------------------------------

# Reduces the manifests to a recipe; it stays the same as long as the
# dependencies do, whatever else changed in the context.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# --------------------------------------------

FROM chef AS builder

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

# Dependencies only, with the same profile, target and features as the build
# below. No cache mounts: the compiled dependencies must land in this layer.
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --target "$(cat /rust-target)" --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Declared after the dependency layer so a new hash only rebuilds the crate.
ARG BUILD_HASH
ENV BUILD_HASH=${BUILD_HASH}
RUN target="$(cat /rust-target)" \
    && cargo build --release --locked --target "$target" \
    && cp "target/$target/release/github-actions-cache-server" /usr/local/bin/

# --------------------------------------------

# glibc, CA certificates and nothing else.
FROM gcr.io/distroless/cc-debian12 AS runner

COPY --from=builder /usr/local/bin/github-actions-cache-server /usr/local/bin/github-actions-cache-server

ENV PORT=3000
EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/github-actions-cache-server"]
