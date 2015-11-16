// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	//"strconv"

	//"github.com/rod6/rodis/resp"
	//"github.com/rod6/rodis/storage"
)

// Implement for command list in http://redis.io/commands#list

/*
func lpush(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) <= 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "lpush").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	keyExists, tipe, expireAt := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	l := ex.DB.ListPush(v[0], v[1:].ToBytes())
	return resp.Integer(l).WriteTo(ex.Buffer)
}
*/
