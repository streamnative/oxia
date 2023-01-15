# Copyright 2023 StreamNative, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.19-alpine as build

RUN apk add --no-cache protobuf make git build-base bash

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

ENV PATH=$PATH:/go/bin
ADD . /src/oxia

RUN cd /src/oxia \
    && make

FROM alpine:3.16.1

RUN apk add --no-cache bash bash-completion

RUN mkdir /oxia
WORKDIR /oxia

COPY --from=build /src/oxia/bin/oxia /oxia/bin/oxia
ENV PATH=$PATH:/oxia/bin

RUN oxia completion bash > ~/.bashrc

CMD /bin/bash