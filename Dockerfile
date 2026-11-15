# syntax=docker/dockerfile:1

FROM rustserverless/lambda-rust@sha256:f0bbeb9b20b4d9bd172a7cae3171b4058bce33932f7ccb61f00995da40401901 AS base

ARG TARGETARCH
ARG WASMTIME_VERSION="50.0.1"
ARG CARGO_LAMBDA_VERSION="v1.0.3"
ARG ZIG_VERSION="0.14.0"

RUN yum install -y gzip
RUN yum install -y tar
RUN yum install -y xz

ENV RUSTUP_HOME="/rustup"
ENV CARGO_HOME="/cargo"
ENV PATH="/cargo/bin:${PATH}"

# wasmtime
RUN case "${TARGETARCH}" in \
      amd64) wasmtime_arch="x86_64"; wasmtime_sha256="685466455dd42a14774a86c36c8f2e45cbb9bd6d92a850139315df60a75498a5" ;; \
      arm64) wasmtime_arch="aarch64"; wasmtime_sha256="d737e077416d78fbfb0a008229ec27d6a0fd5a4924874b8c38b293c850cf4094" ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && wasmtime_archive="wasmtime-v${WASMTIME_VERSION}-${wasmtime_arch}-linux.tar.xz" \
 && curl -fsSL -o "/tmp/${wasmtime_archive}" "https://github.com/bytecodealliance/wasmtime/releases/download/v${WASMTIME_VERSION}/${wasmtime_archive}" \
 && echo "${wasmtime_sha256}  /tmp/${wasmtime_archive}" | sha256sum -c - \
 && tar -xJf "/tmp/${wasmtime_archive}" -C /tmp \
 && mv "/tmp/wasmtime-v${WASMTIME_VERSION}-${wasmtime_arch}-linux/wasmtime" /usr/local/bin/ \
 && rm -rf "/tmp/${wasmtime_archive}" "/tmp/wasmtime-v${WASMTIME_VERSION}-${wasmtime_arch}-linux"
# cargo lambda
RUN case "${TARGETARCH}" in \
      amd64) cargo_lambda_arch="x86_64-unknown-linux-musl"; cargo_lambda_sha256="dd51eb85c541663238b42f3ba50b97c9cf896509880377272a538b55d31530cd" ;; \
      arm64) cargo_lambda_arch="aarch64-unknown-linux-musl"; cargo_lambda_sha256="99183fd9d956675e116c4883ab4d305a5dd4eb54ced6317ac22c079c37d5a5fb" ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && cargo_lambda_archive="cargo-lambda-${CARGO_LAMBDA_VERSION}.${cargo_lambda_arch}.tar.gz" \
 && curl -fsSL -o "/tmp/${cargo_lambda_archive}" "https://github.com/cargo-lambda/cargo-lambda/releases/download/${CARGO_LAMBDA_VERSION}/${cargo_lambda_archive}" \
 && echo "${cargo_lambda_sha256}  /tmp/${cargo_lambda_archive}" | sha256sum -c - \
 && tar -xzf "/tmp/${cargo_lambda_archive}" -C /tmp \
 && mv /tmp/cargo-lambda /cargo/bin/ \
 && rm "/tmp/${cargo_lambda_archive}"

# zig
RUN case "${TARGETARCH}" in \
      amd64) zig_arch="x86_64"; zig_sha256="473ec26806133cf4d1918caf1a410f8403a13d979726a9045b421b685031a982" ;; \
      arm64) zig_arch="aarch64"; zig_sha256="ab64e3ea277f6fc5f3d723dcd95d9ce1ab282c8ed0f431b4de880d30df891e4f" ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && zig_archive="zig-linux-${zig_arch}-${ZIG_VERSION}.tar.xz" \
 && curl -fsSL -o "/tmp/${zig_archive}" "https://ziglang.org/download/${ZIG_VERSION}/${zig_archive}" \
 && echo "${zig_sha256}  /tmp/${zig_archive}" | sha256sum -c - \
 && tar -xJf "/tmp/${zig_archive}" -C /tmp \
 && mv "/tmp/zig-linux-${zig_arch}-${ZIG_VERSION}/zig" /usr/local/bin/ \
 && chmod 755 /usr/local/bin/zig \
 && rm -rf "/tmp/${zig_archive}" "/tmp/zig-linux-${zig_arch}-${ZIG_VERSION}"



FROM base AS builder

WORKDIR /app
COPY ./crates ./crates
COPY ./proto ./proto
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./rust-toolchain.toml ./rust-toolchain.toml

RUN cargo lambda build --manifest-path ./crates/build-plane/build-agent/Cargo.toml --release



FROM base AS build-agent

WORKDIR ${LAMBDA_RUNTIME_DIR}
ENV BUILD_AGENT_SOURCE_DIR="${LAMBDA_RUNTIME_DIR}"
COPY ./crates/foundation/errors ./crates/foundation/errors
COPY ./crates/foundation/messages ./crates/foundation/messages
COPY ./crates/foundation/types ./crates/foundation/types
COPY ./crates/sandbox-plane/exit-codes ./crates/sandbox-plane/exit-codes
COPY ./crates/sandbox-plane/runner ./crates/sandbox-plane/runner
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
      --manifest-path ./crates/sandbox-plane/runner/Cargo.toml \
      --release \
      --bin cwasm-invoker \
      --locked \
      --offline \
      --target-dir /tmp/cwasm-invoker-target \
 && cp /tmp/cwasm-invoker-target/release/cwasm-invoker /usr/local/bin/cwasm-invoker \
 && rm -rf /tmp/cwasm-invoker-target

# Prebuild the wasm runner and all of its dependencies. The agent copies this
# target directory into a fresh /tmp directory for every request, so builds only
# rebuild the runner crate and never reuse artifacts from previous user code.
RUN cargo build \
      --manifest-path ./crates/sandbox-plane/runner/Cargo.toml \
      --target wasm32-wasip1 \
      --profile agent \
      --lib \
      --locked \
      --offline

COPY --from=builder /app/target/lambda/platform-build-agent/bootstrap .

RUN mkdir -p /home/lambda \
 && chown -R 10001:10001 /cargo /home/lambda

ENV HOME="/home/lambda"
USER 10001:10001

CMD ["bootstrap"]
