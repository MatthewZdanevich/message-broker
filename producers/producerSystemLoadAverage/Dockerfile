FROM gcc:12 as builder
WORKDIR /app

COPY main.c .
RUN gcc -o producer main.c -Wall -Wextra


FROM debian:12-slim
WORKDIR /app

COPY --from=builder /app/producer .

RUN apt-get update && apt-get install -y \
    libc6 \
    && rm -rf /var/lib/apt/lists/*

ENV SERVER_IP=127.0.0.1
ENV SERVER_PORT=8080

CMD ["./producer"]