# ---------- Build Stage ----------
FROM golang:1.26 AS builder
WORKDIR /app
COPY . .
RUN go build -o hello-service
# ---------- Runtime Stage ----------
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/hello-service .
EXPOSE 8080
CMD ["./hello-service"]