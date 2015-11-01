// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"github.com/rod6/rodis/server/resp"
	"github.com/syndtr/goleveldb/leveldb"
)

// Implement for command list in http://redis.io/commands#generic

func del(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) == 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "del").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	count := 0
	for _, key := range v {
		_, err := ex.DB.Get(key)
		if err != nil && err != leveldb.ErrNotFound {
			return err
		}
		if err == leveldb.ErrNotFound {
			continue
		}
		if err := ex.DB.Delete(key); err != nil {
			return err
		}
		count++
	}
	return resp.Integer(count).WriteTo(ex.Buffer)
}
