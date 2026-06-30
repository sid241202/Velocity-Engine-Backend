#FROM golang:1.23-alpine AS build
#RUN apk add --no-cache gcc musl-dev pkgconf
#WORKDIR /app
#COPY go.mod ./
#COPY go.sum* ./
#RUN go mod download
#COPY . .
#RUN CGO_ENABLED=1 go build -o /server ./cmd/server

#FROM alpine:3.21
#RUN apk upgrade --no-cache && apk add --no-cache ca-certificates librdkafka
#COPY --from=build /server /server
#EXPOSE 8000
#HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s CMD wget -qO- http://localhost:8000/health || exit 1
#ENTRYPOINT ["/server"]

FROM harbor-registry-non-prod.uidai.gov.in/devops/golang:1.23.3-ubuntu_jammy-gcc-git AS build

# Set module mode + proxy rules
ENV GO111MODULE=on \
    GOPROXY=http://10.10.206.59:8080/repository/go/ \
    GOPRIVATE=bitbucket.uidai.net.in/* \
    GONOPROXY=bitbucket.uidai.net.in/* \
    GONOSUMDB=bitbucket.uidai.net.in

WORKDIR /app

COPY ./.netrc /root/.netrc
COPY ./cyclonedx-gomod /usr/local/bin/cyclonedx-gomod
COPY . .

RUN go build -o velocity-engine-backend cmd/server/main.go

RUN chmod +x /usr/local/bin/cyclonedx-gomod

RUN cyclonedx-gomod app -json -output /SCA-bom.json  -main cmd/server .

FROM harbor-registry-non-prod.uidai.gov.in/devops/golang:1.23.3-ubuntu_jammy-gcc-git

# Create User
RUN useradd -ms /bin/bash uidapp
USER uidapp
WORKDIR /home/uidapp

RUN mkdir -p /home/uidapp/.duckdb/extensions/v1.1.3/linux_amd64
COPY --chown=uidapp:uidapp duckdb-ext/iceberg.duckdb_extension /home/uidapp/.duckdb/extensions/v1.1.3/linux_amd64/
COPY --chown=uidapp:uidapp duckdb-ext/httpfs.duckdb_extension /home/uidapp/.duckdb/extensions/v1.1.3/linux_amd64/

COPY --from=build /SCA-bom.json .
COPY --from=build /app/velocity-engine-backend .
EXPOSE 8000
CMD ["/home/uidapp/velocity-engine-backend"]
