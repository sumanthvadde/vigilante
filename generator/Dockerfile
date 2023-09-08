FROM golang:1.19-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod Download

COPY . .

RUN go build -ldflags="-w -s" -o producer .

FROM scratch
WORKDIR /app
COPY --from=builder /app/producer .

ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_TOPIC=transactions

CMD ["./producer"]