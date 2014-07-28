package main

import (
	"bufio"
	"flag"
	log "github.com/ngaut/logging"
	"github.com/reusee/mmh3"
	"io"
	"net"
)

var addr string
var redisAddr string
var redisInstances = [...]*RedisConnPool{
	NewRedisConnPool("127.0.0.1:6379", 20),
	NewRedisConnPool("127.0.0.1:6380", 20),
	NewRedisConnPool("127.0.0.1:6381", 20),
}

func init() {
	flag.StringVar(&addr, "addr", "0.0.0.0:9037", "proxy address and port, default: 0.0.0.0:9037")
	flag.StringVar(&redisAddr, "redis", "127.0.0.1:6379", "redis address")
}

func redisTunnel(cr *bufio.Reader, cw *bufio.Writer) error {
	// read client request
	resp, err := Parse(cr)
	if err != nil {
		return err
	}
	// TODO
	k, err := resp.Key()
	if err != nil {
		return err
	}

	i := mmh3.Hash32(k) % uint32(len(redisInstances))

	// get redis conn
	redisConn := redisInstances[i].GetConn()
	defer redisInstances[i].ReturnConn(redisConn)

	rr, rw := bufio.NewReader(redisConn), bufio.NewWriter(redisConn)
	if err != io.EOF && err == nil {
		// get resp in bytes
		b, err := resp.Bytes()
		if err != nil {
			return err
		}
		// write to real redis
		rw.Write(b)
		err = rw.Flush()
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	// parse real redis response
	resp, err = Parse(rr)
	if err != io.EOF && err == nil {
		b, err := resp.Bytes()
		if err != nil {
			return err
		}
		cw.Write(b)
		err = cw.Flush()
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

func handleConn(c net.Conn) {
	defer c.Close()
	cr, cw := bufio.NewReader(c), bufio.NewWriter(c)
	for {
		err := redisTunnel(cr, cw)
		if err != nil {
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
