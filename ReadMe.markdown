# Gearman

[![License](http://img.shields.io/badge/license-Simplified_BSD-blue.svg?style=flat)](LICENSE.txt) [![Go Doc](http://img.shields.io/badge/godoc-gearman-blue.svg?style=flat)](http://godoc.org/github.com/nathanaelle/gearman) [![Build Status](https://travis-ci.org/nathanaelle/gearman.svg?branch=master)](https://travis-ci.org/nathanaelle/gearman) [![Go Report Card](https://goreportcard.com/badge/github.com/nathanaelle/gearman)](https://goreportcard.com/report/github.com/nathanaelle/gearman)

## Examples

### Worker Example

```
ctx, cancel := context.WithCancel(context.Background())

w := gearman.NewWorker(ctx, nil)
w.AddServers( gearman.NetConn("tcp","localhost:4730") )

w.AddHandler("reverse", gearman.JobHandler(func(payload io.Reader,reply io.Writer) (error){
        buff	:= make([]byte,1<<16)
        s,_	:= payload.Read(buff)
        buff	= buff[0:s]

        for i:=len(buff); i>0; i-- {
                reply.Write([]byte{ buff[i-1] })
        }

        return nil
} ))

<-ctx.Done()
```

### Client Example

```
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

w := gearman.SingleServerClient(ctx, nil)
w.AddServers( gearman.NetConn("tcp","localhost:4730") )

bytes_val, err_if_any := cli.Submit( NewTask("some task", []byte("some byte encoded payload")) ).Value()
```

## Features

  * Worker Support
  * Client Support
  * Access To Raw Packets
  * Async Client Task with promise

## Protocol Support

### Protocol Plumbing

  The protocol implemented is now  https://github.com/gearman/gearmand/blob/master/PROTOCOL

  * Binary only protocol
  * Access Raw Packets

### Protocol Porcelain

  * PacketFactory for parsing socket
  * Multi server Worker
  * Single server Client
  * Round Robin Client

## License

2-Clause BSD

## Benchmarks

### PacketFactory on LoopReader

```
BenchmarkPacketFactoryPkt0size-4    	30000000	        43.8 ns/op	      20 B/op	       1 allocs/op
BenchmarkPacketFactoryPkt1len-4     	30000000	        53.1 ns/op	      33 B/op	       1 allocs/op
BenchmarkPacketFactoryPktcommon-4   	10000000	       145   ns/op	     128 B/op	       2 allocs/op
```

### Unmarshal

```
BenchmarkUnmarshalPkt0size-4        	100000000	        22.7 ns/op	       8 B/op	       1 allocs/op
BenchmarkUnmarshalPkt1len-4         	30000000	        45.2 ns/op	      48 B/op	       1 allocs/op
BenchmarkUnmarshalPktcommon-4       	20000000	       112   ns/op	      96 B/op	       2 allocs/op
```

### Marshal

```
BenchmarkMarshalPkt0size-4          	300000000	         4.25 ns/op	       0 B/op	       0 allocs/op
BenchmarkMarshalPkt1len-4           	200000000	         9.70 ns/op	       0 B/op	       0 allocs/op
BenchmarkMarshalPktcommon-4         	200000000	         9.81 ns/op	       0 B/op	       0 allocs/op
```
