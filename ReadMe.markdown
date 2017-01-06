# Gearman

[![License](http://img.shields.io/badge/license-Simplified_BSD-blue.svg?style=flat)](LICENSE.txt) [![Go Doc](http://img.shields.io/badge/godoc-gearman-blue.svg?style=flat)](http://godoc.org/github.com/nathanaelle/gearman) [![Build Status](https://travis-ci.org/nathanaelle/gearman.svg?branch=master)](https://travis-ci.org/nathanaelle/gearman)

## Examples

### Worker Example

```
end     := make(chan struct{})
w	:= gearman.NewWorker(end, nil)
w.AddServers( gearman.NetConn("tcp","serveur:1234") )
w.AddHandler("reverse", gearman.JobHandler(func(payload io.Reader,reply io.Writer) (error){
        buff	:= make([]byte,1<<16)
        s,_	:= payload.Read(buff)
        buff	= buff[0:s]

        for i:=len(buff); i>0; i-- {
                reply.Write([]byte{ buff[i-1] })
        }

        return nil
} ))

<-end
```

## Protocol

The protocol implemented is now  https://github.com/gearman/gearmand/blob/master/PROTOCOL

There are some variant :

  * Binary only protocol


## Features

  * [x] Worker Support
  * [x] Client Support
  * [x] Access To Raw Packets
  * [x] Async Client Task with promise
  * [x] Multi server Worker
  * [ ] Multi server Client

## License

2-Clause BSD

## Benchmarks


### PacketFactory on LoopReader

```
BenchmarkPacketFactoryPkt0size-4    	30000000	        48.3 ns/op	      20 B/op	       1 allocs/op
BenchmarkPacketFactoryPkt1len-4     	20000000	        56.6 ns/op	      33 B/op	       1 allocs/op
BenchmarkPacketFactoryPktcommon-4   	10000000	       158 ns/op	     128 B/op	       2 allocs/op
```


### Unmarshal

```
BenchmarkUnmarshalPkt0size-4        	100000000	        22.1 ns/op	       8 B/op	       1 allocs/op
BenchmarkUnmarshalPkt1len-4         	30000000	        47.3 ns/op	      48 B/op	       1 allocs/op
BenchmarkUnmarshalPktcommon-4       	10000000	       119 ns/op	      96 B/op	       2 allocs/op
```


### Marshal

```
BenchmarkMarshalPkt0size-4          	200000000	         6.49 ns/op	       0 B/op	       0 allocs/op
BenchmarkMarshalPkt1len-4           	100000000	        11.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkMarshalPktcommon-4         	100000000	        12.0 ns/op	       0 B/op	       0 allocs/op
```

## Todo

  * Documentation
