# goRainbow
### What it is
go-rainbow is a transformer which accepts your HTTP POST, translates it to metrics and sends it to the wavefront.
It's very light and fast.

### Thanks
A very big thanks to porter-rainbow, which gave us a basic idea about how to design the go-rainbow.

port-rainbow is mainly based on socket connection. go-rainbow provides HTTP connection for you to use.

# Who might consider go-rainbow
1. If you prefer sending message using HTTP POST. 
2. If you have a performance bottle neck in translator.

# High-level
go-rainbow includes 3 parts:
1. http server.
2. translator: translates data into required form, prepared for producer.
3. kafka producer: sends data to kafka(speed-racer).

# Great Features
1. Fast  
    a. Http server handles as less as possible to have a better respond. Leaving heavy work to following modules via channel.  
    b. batch? for producer message?  
2. Light  
    a. Using queue-like channel, rather than cache. We don't need LRU, LFU algorithms here, because each message would be used and only used once.  
# Usage
### Prepare
1. HTTP POST body  
    a. Provide a POST body for rainbow to load your POST request.  
    b. Change it to go struct, Searching "json to go" to build the struct.
2. translator function  
    a. Help rainbow to build metrics as you want.
3. Config file  
    a. "./config/config.json" includes kafka server config, and custom tags.
### Program
The main program is gorainbow.go
```
go run gorainbow.go
```
It will open ports at localhost:7099

# TODO
(From High priority to low)  
1. Exception Handle to be robust.
2. Logging.
3. Performance and memory usage check.
4. Send in seconds, deduplicate feature.
5. Producer is blocking?
6. How to to resend data if fails.