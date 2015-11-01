// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/rod6/log6"

	"github.com/rod6/rodis/server/resp"
	"github.com/rod6/rodis/server/storage"
)

type CommandExtras struct {
	DB           *storage.LevelDB
	Buffer       *bytes.Buffer
	IsConnAuthed bool
	Password     string
}

// command handle function
type commandFunc func(v resp.CommandArgs, ex *CommandExtras) error

// command map attr struct
type attr struct {
	f commandFunc // func for the command
	c int         // arg count for the command
}

// commands, a map type with name as the key
var commands = map[string]*attr{
	// connection
	"auth":   &attr{auth, 2},
	"echo":   &attr{echo, 2},
	"ping":   &attr{ping, 1},
	"select": &attr{selectDB, 2},

	// server
	"flushdb": &attr{flushdb, 1},

	// strings
	"append":      &attr{appendx, 3},
	"bitcount":    &attr{bitcount, 0},
	"bitop":       &attr{bitop, 0},
	"bitpos":      &attr{bitpos, 0},
	"decr":        &attr{decr, 2},
	"decrby":      &attr{decrby, 3},
	"get":         &attr{get, 2},
	"getbit":      &attr{getbit, 3},
	"getrange":    &attr{getrange, 4},
	"getset":      &attr{getset, 3},
	"incr":        &attr{incr, 2},
	"incrby":      &attr{incrby, 3},
	"incrbyfloat": &attr{incrbyfloat, 3},
	"mget":        &attr{mget, 0},
	"mset":        &attr{mset, 0},
	"msetnx":      &attr{msetnx, 0},
	"set":         &attr{set, 0},
	"setbit":      &attr{setbit, 4},
	"setnx":       &attr{setnx, 3},
	"setrange":    &attr{setrange, 4},
	"strlen":      &attr{strlen, 2},

	// keys
	"del": &attr{del, 0},
}

// Get command handler
func findCmdFunc(c string) (*attr, error) {
	a, ok := commands[c]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`cannot find command '%s'`, c))
	}
	return a, nil
}

// Handle command
func Handle(v resp.Array, ex *CommandExtras) error {
	ex.Buffer.Truncate(0) // Truncate all data in the buffer

	if len(v) == 0 {
		log6.Debug("Command handler, len of the input array is 0")
		return resp.NewError(ErrFmtNoCommand).WriteTo(ex.Buffer)
	}

	args := v.ToArgs()
	//log6.Debug("Command handling:%v", humanArgs(args))

	cmd := strings.ToLower(args[0].String())
	a, err := findCmdFunc(cmd)
	if err != nil {
		log6.Debug("Command handler, cannt found command: %v", cmd)
		return resp.NewError(ErrFmtUnknownCommand, cmd).WriteTo(ex.Buffer)
	}

	if a.c != 0 && len(v) != a.c { //a.c = 0 means to check the number in f
		return resp.NewError(ErrFmtWrongNumberArgument, cmd).WriteTo(ex.Buffer)
	}

	if !ex.IsConnAuthed && ex.Password != "" && cmd != "auth" {
		return resp.NewError(ErrAuthed).WriteTo(ex.Buffer)
	}

	return a.f(args[1:], ex)
}

func humanArgs(args resp.CommandArgs) string {
	s := ""
	for _, a := range args {
		s = s + ` '` + a.String() + `'`
	}
	return s
}

const (
	ErrFmtNoCommand           = `ERR no command`
	ErrFmtUnknownCommand      = `ERR unknown command '%s'`
	ErrFmtWrongNumberArgument = `ERR wrong number of arguments for '%s' command`
	ErrFmtSyntax              = `ERR syntax error`
	ErrAuthed                 = `NOAUTH Authentication required.`
	ErrWrongPassword          = `ERR invalid password`
	ErrNoNeedPassword         = `ERR Client sent AUTH, but no password is set`
	ErrSelectInvalidIndex     = `ERR invalid DB index`
	ErrNotValidInt            = `ERR value is not an integer or out of range`
	ErrNotValidFloat          = `ERR value is not a valid float`
	ErrBitOPNotError          = `ERR BITOP NOT must be called with a single source key.`
	ErrSyntax                 = `ERR syntax error`
	ErrShouldBe0or1           = `ERR The bit argument must be 1 or 0.`
	ErrBitOffsetInvalid       = `ERR bit offset is not an integer or out of range`
	ErrBitValueInvalid        = `ERR bit is not an integer or out of range`
	ErrStringExccedLimit      = `ERR string exceeds maximum allowed size (512MB)`
	ErrOffsetOutRange         = `ERR offset is out of range`
)
