
FROM golang:1.19-alpine as build

RUN apk add --no-cache protobuf make git build-base bash

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

ENV PATH=$PATH:/go/bin
ADD . /src/oxia

RUN cd /src/oxia \
    && make

FROM alpine:3.16.1

RUN mkdir /oxia
WORKDIR /oxia

COPY --from=build /src/oxia/bin/oxia /oxia/bin/oxia
ENV PATH=$PATH:/oxia/bin

