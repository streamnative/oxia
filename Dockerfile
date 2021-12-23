
FROM alpine:3.15.0

RUN mkdir /oxia
WORKDIR /oxia

COPY oxia /oxia/

