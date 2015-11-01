// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"github.com/rod6/rodis/server/resp"
	"github.com/rod6/rodis/server/storage"
	"strconv"
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
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

func echo(v resp.CommandArgs, ex *CommandExtras) error {
	return v[0].WriteTo(ex.Buffer)
}

func ping(v resp.CommandArgs, ex *CommandExtras) error {
	return resp.PongSimpleString.WriteTo(ex.Buffer)
}

func selectDB(v resp.CommandArgs, ex *CommandExtras) error {
	s := v[0].String()
	index, err := strconv.Atoi(s)
	if err != nil {
		return resp.NewError(ErrSelectInvalidIndex).WriteTo(ex.Buffer)
	}

	if index < 0 || index > 15 {
		return resp.NewError(ErrSelectInvalidIndex).WriteTo(ex.Buffer)
	}
	ex.DB = storage.SelectStorage(index)
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}
