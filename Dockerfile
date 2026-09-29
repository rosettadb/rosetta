FROM alpine:latest

RUN  apk update \
  && apk upgrade \
  && apk add --update openjdk11 tzdata curl unzip bash \
  && rm -rf /var/cache/apk/*

RUN mkdir /opt/rosetta
COPY binary/build/image/binary-linux_x64 /opt/rosetta/
