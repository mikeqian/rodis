// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
    "bytes"
    "fmt"

    "github.com/rod6/log6"
    "github.com/syndtr/goleveldb/leveldb"

    "github.com/rod6/rodis/server/resp"
    "github.com/rod6/rodis/server/storage"
)

func set(buffer *bytes.Buffer, db *storage.LevelDB, v resp.RESPArray) error {
    if len(v) != 2 {
        log6.Debug(`Command 'set', args number is wrong, it is '%v'`, len(v))

        r := resp.RESPError(fmt.Sprintf(ErrFmtWrongNumberArgument, "set"))
        return r.WriteTo(buffer)
    }

    if err := db.Put([]byte(v[0].(resp.RESPBulkString).String()), []byte(v[1].(resp.RESPBulkString).String())); err != nil {
        log6.Debug(`Command 'set', db backend error :%v`, err)
        return err
    }

    log6.Debug(`Command 'set', return normally`)
    return OK.WriteTo(buffer)
}

func get(buffer *bytes.Buffer, db *storage.LevelDB, v resp.RESPArray) error {
    if len(v) != 1 {
        log6.Debug(`Command 'get', args number is wrong, it is '%v'`, len(v))

        r := resp.RESPError(fmt.Sprintf(ErrFmtWrongNumberArgument, "get"))
        return r.WriteTo(buffer)
    }

    val, err := db.Get([]byte(v[0].(resp.RESPBulkString).String()))
    if err != nil {
        log6.Debug(`Command 'get', db backend error: %v`, err)

        if err == leveldb.ErrNotFound {
            return resp.RESPBulkString(nil).WriteTo(buffer)
        }
        return err
    }

    log6.Debug(`Command 'get', return normally, result is '%v'`, string(val))
    return resp.RESPBulkString(val).WriteTo(buffer)
}
