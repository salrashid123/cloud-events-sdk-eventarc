FROM golang:1.15 as build

ENV GO111MODULE=on

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build http_server.go

FROM gcr.io/distroless/base
COPY --from=build /app/http_server /

EXPOSE 8080

ENTRYPOINT ["/http_server"]
