FROM golang:1.26-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /greeting-etcd-controller .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /greeting-etcd-controller /greeting-etcd-controller
USER nonroot:nonroot
ENTRYPOINT ["/greeting-etcd-controller"]
