
FROM golang:1.19-alpine as build

RUN apk add --no-cache protobuf make git build-base bash

RUN go install github.com/golang/protobuf/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

ENV PATH=$PATH:/go/bin
ADD . /oxia-src

RUN cd /oxia-src \
    && make

FROM alpine:3.16.1

RUN mkdir /oxia
WORKDIR /oxia

COPY --from=build /oxia-src/bin/oxia /oxia/bin/oxia
ENV PATH=$PATH:/oxia/bin

