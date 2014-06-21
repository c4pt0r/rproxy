package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
)

var addr string
var redisAddr string

func init() {
	flag.StringVar(&addr, "addr", "0.0.0.0:9037", "proxy listen address and port, default is 0.0.0.0:9037")
	flag.StringVar(&redisAddr, "redis", "127.0.0.1:6379", "redis addr")
}

func redisTunnel(r *bufio.Reader, w *bufio.Writer) {
	for {
		resp, err := Parse(r)
		if err != io.EOF {
			b, err := resp.Bytes()
			if err != nil {
				log.Println(err)
				return
			}
			w.Write(b)
			w.Flush()
		} else if err != nil {
			// conn close
			log.Println(err)
			return
		}
	}
}

func handleConn(c net.Conn) {
	defer c.Close()
	// create redis conn
	redisConn, err := net.Dial("tcp", redisAddr)
	if err != nil {
		redisConn.Write([]byte("redis error"))
		return
	}

	cr, cw := bufio.NewReader(c), bufio.NewWriter(c)
	rr, rw := bufio.NewReader(redisConn), bufio.NewWriter(redisConn)

	// read client request
	go redisTunnel(cr, rw)
	// write result
	redisTunnel(rr, cw)
}

func runServer(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConn(conn)
	}
}

func main() {
	runServer(addr)
}
