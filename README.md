# goRainbow
go-rainbow is a transformer for sending your metrics to the Wavefront.   
It's very light and fast.

A very big thanks to porter-rainbow, which provides us a basic idea about how to design the go-rainbow.

port-rainbow is mainly based on socket connection. go-rainbow provides HTTP connection for you to use.

# Who should ocnsider go-rainbow
1. If you prefer sending message using HTTP Endpoint. 
2. If you have a performance bottle neck in transformer.

# High-level
go-rainbow includes 3 parts:
1. http server.
2. translator: translates data into required form, prepared for producer.
3. kafka producer: sends data to kafka(speed-racer).

# Great Features
1. Fast:
    a. Http server handles as less as possible to have a better respond. Leaving heavy work to following modules via channel.
    b. batch? for producer message?
2. Light:
    a. Using queue-like channel, rather than cache. We don't need LRU, LFU algorithms here, because each message would be used and only used once. 
# Usage
The main program is gorainbow.go
```
go run gorainbow.go
```