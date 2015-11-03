// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"github.com/rod6/rodis/resp"
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
		xkey, exists, _, _, err := ex.DB.Has(key)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if err := ex.DB.DeleteX(xkey); err != nil {
			return err
		}
		count++
	}
	return resp.Integer(count).WriteTo(ex.Buffer)
}

func exists(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) == 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "exists").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	count := 0
	for _, key := range v {
		_, exists, _, _, err := ex.DB.Has(key)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		count ++
	}
	return resp.Integer(count).WriteTo(ex.Buffer)
}

func tipe(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	_, exists, tipe, _, err := ex.DB.Has(v[0])
	if err != nil {
		return err
	}

	if !exists {
		return resp.SimpleString(TypeString[None]).WriteTo(ex.Buffer)
	}
	return resp.SimpleString(TypeString[tipe]).WriteTo(ex.Buffer)
}
