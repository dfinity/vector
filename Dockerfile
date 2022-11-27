FROM docker.io/alpine:3.14 AS builder

WORKDIR /vector

COPY target/artifacts/vector-*-unknown-linux-musl*.tar.gz ./
RUN tar -xvf vector-0*-"$(cat /etc/apk/arch)"-unknown-linux-musl*.tar.gz --strip-components=2

RUN mkdir -p /var/lib/vector

FROM docker.io/alpine:3.14
RUN apk --no-cache add ca-certificates tzdata

COPY ./helpers/libz.so.1.2.11 /lib/x86_64-linux-gnu/libz.so.1
COPY ./helpers/libresolv.so.2 /lib/x86_64-linux-gnu/libresolv.so.2
COPY ./helpers/libstdc++.so.6 /lib/x86_64-linux-gnu/libstdc++.so.6
COPY ./helpers/libgcc_s.so.1 /lib/x86_64-linux-gnu/libgcc_s.so.1
COPY ./helpers/libm.so.6 /lib/x86_64-linux-gnu/libm.so.6
COPY ./helpers/libc.so.6 /lib/x86_64-linux-gnu/libc.so.6
COPY ./helpers/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2

COPY --from=builder /vector/bin/* /usr/local/bin/
COPY --from=builder /vector/config/vector.toml /etc/vector/vector.toml
COPY --from=builder /var/lib/vector /var/lib/vector

# Smoke test
RUN ["vector", "--version"]

ENTRYPOINT ["/usr/local/bin/vector"]
