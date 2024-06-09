FROM golang:latest

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY data_loaders data_loaders
COPY manager.go .

RUN go build -o manager .

ENTRYPOINT ["./manager"]