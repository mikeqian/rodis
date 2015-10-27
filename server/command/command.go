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


// command handle function
type commandFunc func(buffer *bytes.Buffer, db *storage.LevelDB, v resp.RESPArray) (error)

// commands, a map type with name as the key
var commands = map[string] commandFunc {
    "get": get,
    "set": set,
}

// Get command handler
func findCmdFunc(c string) (commandFunc, error) {
    f, ok := commands[strings.ToLower(c)]
    if !ok {
        return nil, errors.New(fmt.Sprintf(`cannot find command '%s'`, c))
    }
    return f, nil
}

// Handle command
func Handle(buffer *bytes.Buffer, db *storage.LevelDB, v resp.RESPArray) error {
    buffer.Truncate(0)   // Truncate all data in the buffer

    if len(v) == 0 {
        log6.Debug("Command handler, len of the input array is 0")

        r := resp.RESPError(fmt.Sprintf(ErrFmtNoCommand))
        return r.WriteTo(buffer)
    }

    cmd := v[0].(resp.RESPBulkString).String()
    log6.Debug("Command handler, cmd is '%s'", cmd)

    f, err := findCmdFunc(cmd)
    if err != nil {
        log6.Debug("Command handler, cannt found command: %v", cmd)

        r := resp.RESPError(fmt.Sprintf(ErrFmtUnknownCommand, cmd))
        return r.WriteTo(buffer)
    }

    args := v[1:]
    return f(buffer, db, args)
}

const (
    ErrFmtNoCommand             = `ERR no command`
    ErrFmtUnknownCommand        = `ERR unknown command '%s'`
    ErrFmtWrongNumberArgument   = `ERR wrong number of arguments for '%s' command`
    ErrFmtSyntax                = `Err syntax error`
)

const (
    OK  = resp.RESPSimpleString("OK")
)
