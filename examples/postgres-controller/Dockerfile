FROM golang:1.26-alpine AS build

WORKDIR /repo

# Copy the parent module (needed for replace directive).
COPY go.mod go.sum ./
COPY internal/ internal/
COPY pkg/ pkg/
COPY test/ test/

# Copy the postgres-controller module.
COPY examples/postgres-controller/ examples/postgres-controller/

WORKDIR /repo/examples/postgres-controller
RUN go mod download
RUN CGO_ENABLED=0 go build -o /greeting-postgres-controller .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /greeting-postgres-controller /greeting-postgres-controller
USER nonroot:nonroot
ENTRYPOINT ["/greeting-postgres-controller"]
