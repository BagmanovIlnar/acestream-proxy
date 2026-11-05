FROM rust:1 AS build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/acestream-proxy /usr/local/bin/acestream-proxy
ENV TMP_DIR=/tmp/acestream-proxy
EXPOSE 8080
ENTRYPOINT ["acestream-proxy"]
