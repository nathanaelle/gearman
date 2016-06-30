# Gearman

![License](http://img.shields.io/badge/license-Simplified_BSD-blue.svg?style=flat) [![Go Doc](http://img.shields.io/badge/godoc-gearman-blue.svg?style=flat)](http://godoc.org/github.com/nathanaelle/gearman) [![Build Status](https://travis-ci.org/nathanaelle/gearman.svg?branch=master)](https://travis-ci.org/nathanaelle/gearman)

## Examples

## Features

  * [x] Worker Support
  * [x] Client Support
  * [x] Async Task with promise
  * [x] Multi server Worker
  * [ ] Multi server Client

## License

2-Clause BSD

## Benchmarks

### Read and decode

```
BenchmarkReadPkt0size-4    	20000000	       68.5 ns/op	      24 B/op	       2 allocs/op
BenchmarkReadPkt1len-4     	10000000	       137 ns/op	      48 B/op	       3 allocs/op
BenchmarkReadPktcommon-4   	 5000000	       277 ns/op	     144 B/op	       4 allocs/op
```


### Encode

```
BenchmarkMarshalPkt0size-4 	100000000	        14.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkMarshalPkt1len-4  	100000000	        22.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkMarshalPktcommon-4	100000000	        22.9 ns/op	       0 B/op	       0 allocs/op
```


## Todo

  * Documentation
  * Comments
