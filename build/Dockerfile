# adalrsjr1/clustersql
FROM golang:1.19-buster AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY ../ ./

# using -ldflags to reduce binary size
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags '-w -s' -o clustersql cmd/app/main.go

## Deploy
FROM scratch

WORKDIR /

COPY --from=build /app/clustersql /clustersql

EXPOSE 3306

ENTRYPOINT ["/clustersql"]
