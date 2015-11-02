package main

import (
	"testing"
)

func TestDel(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"del"}, replyType{"Error", "ERR wrong number of arguments for 'del' command"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"del", "a", "b"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"del", "a", "b", "c"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "b", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "c", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"del", "a", "b", "c", "d"}, replyType{"Integer", int64(2)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", nil}},
		{[]interface{}{"get", "b"}, replyType{"BulkString", nil}},
		{[]interface{}{"get", "c"}, replyType{"BulkString", nil}},
	}
	runTest("DEL", tests, t)
}
