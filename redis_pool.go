package main

import (
	log "github.com/ngaut/logging"
	"net"
)

type RedisConnPool struct {
	addr     string
	poolSize int
	pool     chan net.Conn
}

func NewRedisConnPool(addr string, size int) *RedisConnPool {
	p := &RedisConnPool{
		addr:     addr,
		poolSize: size,
		pool:     make(chan net.Conn, size),
	}
	for i := 0; i < size; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		p.pool <- c
	}
	return p
}

func (p *RedisConnPool) GetConn() net.Conn {
	c := <-p.pool
	return c
}

func (p *RedisConnPool) ReturnConn(c net.Conn) {
	if c != nil {
		p.pool <- c
	} else {
		// if conn occur error, user just return nil
		c, err := net.Dial("tcp", addr)
		if err != nil {
			log.Warning(err)
		}
		p.pool <- c
	}
}
