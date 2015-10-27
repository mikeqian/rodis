// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package resp

import (
    "bytes"
    "fmt"
)

type RESPType int
const (
    RESPWrongType        = iota     // wrong input
    RESPSimpleStringType
    RESPErrorType
    RESPIntegerType
    RESPBulkStringType
    RESPArrayType
)

type RESPValue interface {
	WriteTo(*bytes.Buffer) error
}

// RESP SimpleString
type RESPSimpleString string

func (s RESPSimpleString) WriteTo(w *bytes.Buffer) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", s)
	return err
}

func (s RESPSimpleString) String() string {
	return string(s)
}

// RESP Integer
type RESPInteger int64

func (i RESPInteger) WriteTo(w *bytes.Buffer) error {
	_, err := fmt.Fprintf(w, ":%d\r\n", i)
	return err
}

// RESP Error
type RESPError string

func (e RESPError) Error() string {
	return string(e)
}

func (e RESPError) WriteTo(w *bytes.Buffer) error {
	_, err := fmt.Fprintf(w, "-%s\r\n", e)
	return err
}

// RESP Bulk String
type RESPBulkString []byte

func (b RESPBulkString) WriteTo(w *bytes.Buffer) error {
	if b == nil {
		_, err := fmt.Fprintf(w, "$-1\r\n")
		return err
	}

	if _, err := fmt.Fprintf(w, "$%d\r\n", len(b)); err != nil {
		return err
	}

	w.Write(b)
	w.WriteString("\r\n")

	return nil
}

func (b RESPBulkString) String() string {
    if b == nil {
        return ""
    }

    return string(b)
}

// RESP Array
type RESPArray []RESPValue

func (a RESPArray) WriteTo(w *bytes.Buffer) error {
	if a == nil {
		_, err := fmt.Fprintf(w, "*-1\r\n")
		return err
	}

	if _, err := fmt.Fprintf(w, "*%d\r\n", len(a)); err != nil {
        return err
    }

	for _, v := range a {
		if err := v.WriteTo(w); err != nil {
            return err
        }
	}

	return nil
}
