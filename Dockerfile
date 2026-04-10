FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /kigeon ./cmd/kigeon

FROM gcr.io/distroless/static-debian12

COPY --from=builder /kigeon /kigeon

ENTRYPOINT ["/kigeon"]
