// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
    "github.com/rod6/log6"
    "github.com/syndtr/goleveldb/leveldb"

    "github.com/rod6/rodis/server/resp"
)

func set(v resp.CommandArgs, ex *CommandExtras) error {
    if err := ex.DB.Put(v[0], v[1]); err != nil {
        log6.Debug(`Command 'set', db backend error :%v`, err)
        return err
    }
    return resp.OK.WriteTo(ex.Buffer)
}

func get(v resp.CommandArgs, ex *CommandExtras) error {
    val, err := ex.DB.Get(v[0])
    if err != nil {
        log6.Debug(`Command 'get', db backend error: %v`, err)
        if err == leveldb.ErrNotFound {
            return resp.BulkString(nil).WriteTo(ex.Buffer)
        }
        return err
    }
    return resp.BulkString(val).WriteTo(ex.Buffer)
}
