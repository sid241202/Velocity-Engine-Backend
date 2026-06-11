FROM golang:1.23-alpine AS build
RUN apk add --no-cache gcc musl-dev pkgconf
WORKDIR /app
COPY go.mod ./
COPY go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /server ./cmd/server

FROM alpine:3.21
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates librdkafka
COPY --from=build /server /server
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s CMD wget -qO- http://localhost:8000/health || exit 1
ENTRYPOINT ["/server"]
