# syntax=docker/dockerfile:1

FROM rustserverless/lambda-rust:latest AS base

ARG TARGETARCH
ARG CARGO_LAMBDA_VERSION="v1.0.3"
ARG ZIG_VERSION="0.14.0"

RUN yum install -y gzip
RUN yum install -y tar
RUN yum install -y xz

ENV RUSTUP_HOME="/usr/local/rustup"
ENV CARGO_HOME="/usr/local/cargo"
ENV PATH="/usr/local/cargo/bin:${PATH}"
ENV PATH="/cargo/bin:/root/.wasmtime/bin:${PATH}"

# wasmtime
RUN case "${TARGETARCH}" in \
      amd64) wasmtime_arch="x86_64" ;; \
      arm64) wasmtime_arch="aarch64" ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && curl -LO "https://github.com/bytecodealliance/wasmtime/releases/download/v43.0.1/wasmtime-v43.0.1-${wasmtime_arch}-linux.tar.xz" \
 && tar -xf "wasmtime-v43.0.1-${wasmtime_arch}-linux.tar.xz" \
 && mv "wasmtime-v43.0.1-${wasmtime_arch}-linux/wasmtime" /usr/local/bin/ \
 && rm -rf "wasmtime-v43.0.1-${wasmtime_arch}-linux"*
# cargo lambda
RUN case "${TARGETARCH}" in \
      amd64) cargo_lambda_arch="x86_64-unknown-linux-musl" ;; \
      arm64) cargo_lambda_arch="aarch64-unknown-linux-musl" ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && curl -fL "https://github.com/cargo-lambda/cargo-lambda/releases/download/${CARGO_LAMBDA_VERSION}/cargo-lambda-${CARGO_LAMBDA_VERSION}.${cargo_lambda_arch}.tar.gz" -o /tmp/cargo-lambda.tar.gz \
 && tar -xzf /tmp/cargo-lambda.tar.gz -C /tmp \
 && mv /tmp/cargo-lambda /cargo/bin/ \
 && rm /tmp/cargo-lambda.tar.gz

# zig
RUN case "${TARGETARCH}" in \
      amd64) zig_arch="x86_64" ;; \
      arm64) zig_arch="aarch64" ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && curl -fLO "https://ziglang.org/download/${ZIG_VERSION}/zig-linux-${zig_arch}-${ZIG_VERSION}.tar.xz" \
 && tar -xf "zig-linux-${zig_arch}-${ZIG_VERSION}.tar.xz" \
 && mv "zig-linux-${zig_arch}-${ZIG_VERSION}/zig" /usr/local/bin/ \
 && chmod 755 /usr/local/bin/zig \
 && rm -rf "zig-linux-${zig_arch}-${ZIG_VERSION}"*



FROM base AS builder

WORKDIR /app
COPY ./crates ./crates
COPY ./proto ./proto
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./rust-toolchain.toml ./rust-toolchain.toml

RUN cargo lambda build --manifest-path ./crates/apps/build-agent/Cargo.toml --release



FROM base AS build-agent

WORKDIR ${LAMBDA_RUNTIME_DIR}
ENV BUILD_AGENT_SOURCE_DIR="${LAMBDA_RUNTIME_DIR}"
COPY ./crates/compute/exit_codes ./crates/compute/exit_codes
COPY ./crates/compute/runner ./crates/compute/runner
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./rust-toolchain.toml ./rust-toolchain.toml
COPY ./.cargo ./.cargo

# Vendor the dependencies needed by the runner-only workspace. The agent uses
# these vendored crates at runtime, so it never needs the private registry or
# any registry credentials.
RUN cargo vendor vendor > /tmp/vendor-config.toml \
 && cat /tmp/vendor-config.toml >> .cargo/config.toml \
 && rm /tmp/vendor-config.toml \
 && sed -i "s#directory = \"vendor\"#directory = \"$(pwd)/vendor\"#" .cargo/config.toml

# Install the native host used to run algorithms.
RUN cargo build \
      --manifest-path ./crates/compute/runner/Cargo.toml \
      --release \
      --bin build-runner \
      --locked \
      --offline \
      --target-dir /tmp/build-runner-target \
 && cp /tmp/build-runner-target/release/build-runner /usr/local/bin/build-runner \
 && rm -rf /tmp/build-runner-target

# Prebuild the wasm runner and all of its dependencies. The agent copies this
# target directory into a fresh /tmp directory for every request, so builds only
# rebuild the runner crate and never reuse artifacts from previous user code.
RUN cargo build \
      --manifest-path ./crates/compute/runner/Cargo.toml \
      --target wasm32-wasip1 \
      --release \
      --lib \
      --locked \
      --offline

COPY --from=builder /app/target/lambda/platform-build-agent/bootstrap .

CMD ["bootstrap"]
