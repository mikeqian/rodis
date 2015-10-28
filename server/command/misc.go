// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
    "github.com/rod6/rodis/server/resp"
)

func auth(v resp.CommandArgs, ex *CommandExtras) error {
    if ex.Password == "" {
        return resp.NewError(ErrNoNeedPassword).WriteTo(ex.Buffer)
    }
    if v[0].String() != ex.Password {
        ex.IsConnAuthed = false
        return resp.NewError(ErrWrongPassword).WriteTo(ex.Buffer)
    }
    ex.IsConnAuthed = true
    return resp.OK.WriteTo(ex.Buffer)
}

func ping(v resp.CommandArgs, ex *CommandExtras) error {
    return resp.PONG.WriteTo(ex.Buffer)
}
