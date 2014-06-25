package main

import (
	"bufio"
	"flag"
	log "github.com/ngaut/logging"
	"io"
	"net"
)

var addr string
var redisAddr string
var redisPool *RedisConnPool

func init() {
	flag.StringVar(&addr, "addr", "0.0.0.0:9037", "proxy address and port, default: 0.0.0.0:9037")
	flag.StringVar(&redisAddr, "redis", "127.0.0.1:6379", "redis address")
	redisPool = NewRedisConnPool(redisAddr, 20)
}

func redisTunnel(r *bufio.Reader, w *bufio.Writer) {
	for {
		resp, err := Parse(r)
		if err != io.EOF {
			b, err := resp.Bytes()
			if err != nil {
				log.Warning(err)
				return
			}
			w.Write(b)
			w.Flush()
		} else if err != nil {
			log.Warning(err)
			return
		}
	}
}

func handleConn(c net.Conn) {
	defer c.Close()
	// create redis conn
	cr, cw := bufio.NewReader(c), bufio.NewWriter(c)

	// write result
	// read client request
	for {
		resp, err := Parse(cr)
		redisConn := redisPool.GetConn()
		defer redisPool.ReturnConn(redisConn)
		rr, rw := bufio.NewReader(redisConn), bufio.NewWriter(redisConn)
		go redisTunnel(rr, cw)
		// TODO run redis hook
		if err != io.EOF {
			// get resp in bytes
			b, err := resp.Bytes()
			if err != nil {
				log.Warning(err)
				return
			}
			// write to real redis
			rw.Write(b)
			rw.Flush()
		} else if err != nil {
			log.Warning(err)
			return
		}
	}
}

func runServer(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Warning(err)
			continue
		}
		go handleConn(conn)
	}
}

func main() {
	runServer(addr)
}
