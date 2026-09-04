FROM golang:1.25-alpine3.23 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go test ./...

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -trimpath -ldflags="-s -w" -o cdc-cli ./cmd/cli

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -trimpath -ldflags="-s -w" -o cdc-server ./cmd/server

FROM alpine:3.23

WORKDIR /app

RUN apk upgrade --no-cache \
	&& apk add --no-cache ca-certificates \
	&& addgroup -S -g 65532 tabellarius \
	&& adduser -S -D -H -u 65532 -G tabellarius tabellarius

COPY --from=builder /app/cdc-cli /usr/local/bin/cdc-cli
COPY --from=builder /app/cdc-server /usr/local/bin/cdc-server

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/cdc-server"]
