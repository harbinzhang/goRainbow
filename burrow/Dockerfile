FROM golang:1.10.0-alpine3.7 as builder

ENV DEP_VERSION="0.3.2"

RUN apk update && apk add bash && \
	apk add openjdk8 



# For Kafka related
RUN apk add --update --no-cache alpine-sdk bash ca-certificates \
      libressl \
      tar \
      git openssh openssl yajl-dev zlib-dev cyrus-sasl-dev openssl-dev build-base coreutils
WORKDIR /root
RUN git clone https://github.com/edenhill/librdkafka.git
WORKDIR /root/librdkafka
RUN /root/librdkafka/configure
RUN make
RUN make install
#For golang applications
RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2
RUN mkdir /app

ENV RAINBOW "harbinzhang/goRainbow"
WORKDIR /go/src/github.com/

RUN go get -d -v github.com/confluentinc/confluent-kafka-go/kafka
RUN git clone https://github.com/harbinzhang/goRainbow.git
RUN go get -d -v github.com/${RAINBOW}

WORKDIR /go/src/github.com/${RAINBOW}
RUN GOOS=linux go build -a -installsuffix cgo -o /app/rainbow .
RUN mv ./config /app/config
# COPY /goRainbow ./
# COPY /goRainbow/config /app/config
# RUN GOOS=linux go build -a -installsuffix cgo -o /app/rainbow . && \
# 	chmod -R 777 /app/rainbow

ENV TZ America/Los_Angeles


# For Burrow related
RUN apk add --no-cache git curl tzdata && \
	curl -L -s https://github.com/golang/dep/releases/download/v${DEP_VERSION}/dep-linux-amd64 -o $GOPATH/bin/dep && \
	chmod +x $GOPATH/bin/dep && \
	mkdir -p $GOPATH/src/github.com/linkedin/Burrow

ADD . $GOPATH/src/github.com/linkedin/Burrow/

RUN cd $GOPATH/src/github.com/linkedin/Burrow && \
	dep ensure && \
	go build -o /app/burrow .


WORKDIR /app
# COPY --from=builder /tmp/burrow /app/
ADD /docker-config/docker-entrypoint.sh /etc/burrow/
ADD /docker-config/burrow.toml /etc/burrow/
ADD /conf/http-post.tmpl /etc/burrow/conf/
ADD /conf/slack-post.tmpl /etc/burrow/conf/
ADD /conf/slack-delete.tmpl /etc/burrow/conf/
RUN mkdir -p /etc/burrow/logs

RUN chmod +x /etc/burrow/docker-entrypoint.sh

# CMD [ "/app/rainbow" ]
ENTRYPOINT [ "/etc/burrow/docker-entrypoint.sh" ]
# CMD ["bash"]
CMD ["/app/burrow", "--config-dir", "/etc/burrow"]