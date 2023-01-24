FROM messense/rust-musl-cross:x86_64-musl AS builder

WORKDIR /vector

COPY . .
RUN rustup target add x86_64-unknown-linux-musl
RUN apt-get update && apt install -y protobuf-compiler
RUN cargo build --target x86_64-unknown-linux-musl --no-default-features --features target-x86_64-unknown-linux-musl  --release

# COPY target/artifacts/vector-*-unknown-linux-musl*.tar.gz ./
# RUN tar -xvf vector-0*-"$(cat /etc/apk/arch)"-unknown-linux-musl*.tar.gz --strip-components=2

RUN mkdir -p /var/lib/vector

FROM docker.io/alpine:3.14

COPY --from=builder /vector/target/x86_64-unknown-linux-musl/release/vector /usr/local/bin/

# Smoke test
RUN ["vector", "--version"]

ENTRYPOINT ["/usr/local/bin/vector"]
