FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
# benches/ is needed so Cargo can resolve the [[bench]] target declared in Cargo.toml;
# the benchmark itself is not compiled by `cargo build` (only `cargo bench`).
COPY benches/ benches/
RUN cargo build --release --bin deltat

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/deltat /usr/local/bin/deltat
EXPOSE 5433
VOLUME /data
ENV DELTAT_DATA_DIR=/data
ENV DELTAT_BIND=0.0.0.0
CMD ["deltat"]
