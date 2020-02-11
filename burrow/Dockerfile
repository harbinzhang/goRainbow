FROM golang:1.12-alpine as builder

ENV DEP_VERSION="0.3.2"

RUN apk update && apk add bash && \
	apk add openjdk8 

ENV TZ America/Los_Angeles

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


#For goRainbow applications
RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2
RUN mkdir /app

ENV RAINBOW "goRainbow"
WORKDIR /go/src/github.com/

RUN go get -d -v github.com/confluentinc/confluent-kafka-go/kafka

WORKDIR /go/src/github.com
COPY ./goRainbow ./goRainbow

WORKDIR /go/src/github.com/${RAINBOW}
RUN GOOS=linux go build -a -installsuffix cgo -o /app/rainbow .
RUN mv ./config /app/config


# For Burrow related
RUN apk add --no-cache git curl tzdata && \
	curl -L -s https://github.com/golang/dep/releases/download/v${DEP_VERSION}/dep-linux-amd64 -o $GOPATH/bin/dep && \
	chmod +x $GOPATH/bin/dep && \
	mkdir -p $GOPATH/src/github.com/linkedin

RUN cd $GOPATH/src/github.com/linkedin && \
	git clone https://github.com/linkedin/Burrow.git && \
	cd Burrow && \
	git checkout 8403e4299c0af1d5be66404955303936014e9cda

RUN cd $GOPATH/src/github.com/linkedin/Burrow && \
	dep ensure && \
	go build -o /app/burrow .


WORKDIR /app
ADD /docker-config/docker-entrypoint.sh /etc/burrow/
ADD /docker-config/burrow.toml /etc/burrow/
ADD /conf/http-post.tmpl /etc/burrow/conf/
ADD /conf/slack-post.tmpl /etc/burrow/conf/
ADD /conf/slack-delete.tmpl /etc/burrow/conf/
RUN mkdir -p /etc/burrow/logs

RUN chmod +x /etc/burrow/docker-entrypoint.sh

ENTRYPOINT [ "/etc/burrow/docker-entrypoint.sh" ]
CMD ["/app/burrow", "--config-dir", "/etc/burrow"]
