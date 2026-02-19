
FROM golang:1.24.6-alpine AS builder


WORKDIR /app


COPY go.mod go.sum ./


RUN go mod download


COPY . .

RUN go build -o processor ./services/processor/cmd/main.go

FROM alpine:latest


WORKDIR /root/

COPY --from=builder /app/processor .

EXPOSE 2112

CMD ["./processor"]