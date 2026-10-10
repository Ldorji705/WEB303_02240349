FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/gateway-bin ./api-gateway

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S appgroup \
    && adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=builder /out/gateway-bin ./gateway-bin
COPY api-gateway/static ./api-gateway/static

USER appuser

EXPOSE 8080

CMD ["./gateway-bin"]
