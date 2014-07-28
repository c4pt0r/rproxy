package main

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
)

/*
 * redis protocal : RESP protocol
 * http://redis.io/topics/protocol
 */
type RESPType int

const (
	SimpleString RESPType = iota
	Error
	Integer
	Bluk
	Array
)

var ErrInvalid error = errors.New("invalid redis packet")

type RESP struct {
	t     RESPType
	b     []byte
	array []*RESP
}

func Parse(r *bufio.Reader) (*RESP, error) {
	line, _, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	if len(line) > 0 {
		switch line[0] {
		case '*':
			{
				arraySz, err := strconv.Atoi(string(line[1:]))
				var array []*RESP
				if err != nil {
					return nil, err
				}
				for i := 0; i < arraySz; i++ {
					resp, err := Parse(r)
					if err != nil {
						return nil, err
					}
					array = append(array, resp)
				}

				return &RESP{
					t:     Array,
					b:     nil,
					array: array,
				}, nil
			}
		case '+':
			{
				return &RESP{
					t:     SimpleString,
					b:     line[1:],
					array: nil,
				}, nil
			}
		case '-':
			{
				return &RESP{
					t:     Error,
					b:     line[1:],
					array: nil,
				}, nil
			}
		case '$':
			{
				sz, err := strconv.Atoi(string(line[1:]))
				if err != nil {
					return nil, err
				}
				buf := make([]byte, sz)
				_, err = r.Read(buf)
				if err != nil {
					return nil, err
				}
				r.ReadLine()
				return &RESP{
					t:     Bluk,
					b:     buf,
					array: nil,
				}, nil
			}
		case ':':
			{
				return &RESP{
					t:     Integer,
					b:     line[1:],
					array: nil,
				}, nil
			}
		default:
			return nil, ErrInvalid
		}
	}
	return nil, ErrInvalid
}

func (r *RESP) Op() ([]byte, error) {
	if r.t != Array || len(r.array) < 1 {
		return nil, ErrInvalid
	}
	return r.array[0].b, nil
}

func (r *RESP) Key() ([]byte, error) {
	if r.t != Array || len(r.array) < 2 {
		return nil, ErrInvalid
	}
	return r.array[1].b, nil
}

func (r *RESP) Bytes() ([]byte, error) {
	var buf []byte
	switch r.t {
	case SimpleString:
		{
			buf = append(buf, '+')
			buf = append(buf, r.b...)
			buf = append(buf, []byte("\r\n")...)
		}
	case Error:
		{
			buf = append(buf, '-')
			buf = append(buf, r.b...)
			buf = append(buf, []byte("\r\n")...)
		}
	case Integer:
		{
			buf = append(buf, ':')
			buf = append(buf, r.b...)
			buf = append(buf, []byte("\r\n")...)
		}
	case Bluk:
		{
			buf = append(buf, '$')
			buf = append(buf, []byte(strconv.Itoa(len(r.b)))...)
			buf = append(buf, []byte("\r\n")...)
			buf = append(buf, r.b...)
			buf = append(buf, []byte("\r\n")...)
		}
	case Array:
		{
			buf = append(buf, '*')
			buf = append(buf, []byte(strconv.Itoa(len(r.array)))...)
			buf = append(buf, []byte("\r\n")...)
			for _, resp := range r.array {
				b, err := resp.Bytes()
				if err != nil {
					return nil, err
				}
				buf = append(buf, b...)
			}
		}
	}
	return buf, nil
}

func (r *RESP) String() string {
	switch r.t {
	case Bluk:
		return fmt.Sprint("bluk:", r.b)
	case SimpleString:
		return fmt.Sprint("string:", string(r.b))
	case Error:
		return fmt.Sprint("error:", string(r.b))
	case Array:
		return fmt.Sprint("array:", r.array)
	case Integer:
		return fmt.Sprint("int:", string(r.b))
	}
	return "unknown"
}
