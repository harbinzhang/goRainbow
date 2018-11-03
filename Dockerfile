FROM golang:1.11-alpine as builder

ENV DEP_VERSION="0.5.0"
RUN apk update && apk add bash && \
	apk add openjdk8 git && \
    apk add librdkafka librdkafka-dev&& \
    apk add pkgconfig gcc


# RUN git clone https://github.com/edenhill/librdkafka.git && \
#     cd librdkafka && \
#     ./configure --prefix /usr && \
#     make && \
#     sudo make install 

RUN apk add --no-cache git curl tzdata && \
	curl -L -s https://github.com/golang/dep/releases/download/v${DEP_VERSION}/dep-linux-amd64 -o $GOPATH/bin/dep && \
	chmod +x $GOPATH/bin/dep && \
    mkdir -p $GOPATH/src/github.com/HarbinZhang/goRainbow

ENV TZ America/Los_Angeles	

COPY . $GOPATH/src/github.com/HarbinZhang/goRainbow
RUN cd $GOPATH/src/github.com/HarbinZhang/goRainbow/
RUN dep init
RUN	dep ensure
RUN	go build -o /app/gorainbow ./gorainbow

WORKDIR /app

CMD ["bash"]