FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /loadtest ./loadtest/cmd/loadtest/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /loadtest /usr/local/bin/loadtest
ENTRYPOINT ["loadtest"]
