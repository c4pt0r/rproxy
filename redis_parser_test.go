package main

import (
	"bufio"
	"bytes"
	"testing"
)

func TestParser(t *testing.T) {
	sample := "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"
	buf := bytes.NewBuffer([]byte(sample))
	r := bufio.NewReader(buf)

	resp, err := Parse(r)
	if err != nil {
		t.Error(err)
	}
	b, err := resp.Bytes()
	if err != nil {
		t.Error(err)
	}
	if resp == nil {
		t.Error("unknown error")
	}
	if len(b) != len(sample) {
		t.Error("to bytes error")
	}
}
