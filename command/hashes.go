// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"strconv"

	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

// Implement for command list in http://redis.io/commands#hash

func hdel(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "hdel").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fields := [][]byte{}
	for _, field := range v[1:] {
		fields = append(fields, []byte(field))
	}
	hash := ex.DB.GetHashFields(v[0], fields)

	count := 0
	for _, value := range hash {
		if value != nil {
			count++
		}
	}
	ex.DB.DeleteHashFields(v[0], fields)
	return resp.Integer(len(fields)).WriteTo(ex.Buffer)
}

func hexists(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	hash := ex.DB.GetHashFields(v[0], [][]byte{v[1]})
	if hash[string(v[1])] == nil {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	return resp.OneInteger.WriteTo(ex.Buffer)
}

func hget(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetHashFields(v[0], [][]byte{v[1]})
	return resp.BulkString(hash[string(v[1])]).WriteTo(ex.Buffer)
}

func hgetall(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetHash(v[0])
	arr := resp.Array{}

	for field, value := range hash {
		arr = append(arr, resp.BulkString([]byte(field)), resp.BulkString(value))
	}
	return arr.WriteTo(ex.Buffer)
}

func hincrby(v resp.CommandArgs, ex *CommandExtras) error {
	by, err := strconv.ParseInt(v[2].String(), 10, 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	keyExists, tipe, expireAt := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetHashFields(v[0], [][]byte{v[1]})

	newVal := int64(0)
	if hash[string(v[1])] == nil {
		newVal += by
	} else {
		i, err := strconv.ParseInt(string(hash[string(v[1])]), 10, 64)
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}
		newVal = i + by
	}
	hash[string(v[1])] = []byte(strconv.FormatInt(newVal, 10))

	ex.DB.PutHash(v[0], hash, expireAt)
	return resp.Integer(newVal).WriteTo(ex.Buffer)
}

func hincrbyfloat(v resp.CommandArgs, ex *CommandExtras) error {
	by, err := strconv.ParseFloat(v[2].String(), 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	keyExists, tipe, expireAt := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetHashFields(v[0], [][]byte{v[1]})

	newVal := 0.0
	if hash[string(v[1])] == nil {
		newVal += by
	} else {
		f, err := strconv.ParseFloat(string(hash[string(v[1])]), 64)
		if err != nil {
			return resp.NewError(ErrNotValidFloat).WriteTo(ex.Buffer)
		}
		newVal = f + by
	}
	hash[string(v[1])] = []byte(strconv.FormatFloat(newVal, 'f', -1, 64))

	ex.DB.PutHash(v[0], hash, expireAt)
	return resp.BulkString(hash[string(v[1])]).WriteTo(ex.Buffer)
}

func hkeys(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fields := ex.DB.GetHashFieldNames(v[0])
	arr := resp.Array{}

	for _, field := range fields {
		arr = append(arr, resp.BulkString(field))
	}
	return arr.WriteTo(ex.Buffer)
}

func hvals(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetHash(v[0])
	arr := resp.Array{}

	for _, value := range hash {
		arr = append(arr, resp.BulkString([]byte(value)))
	}
	return arr.WriteTo(ex.Buffer)
}

func hlen(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fields := ex.DB.GetHashFieldNames(v[0])
	return resp.Integer(len(fields)).WriteTo(ex.Buffer)
}

func hmget(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "hmget").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fields := [][]byte{}
	for _, field := range v[1:] {
		fields = append(fields, []byte(field))
	}
	hash := ex.DB.GetHashFields(v[0], fields)

	arr := resp.Array{}
	for _, value := range hash {
		arr = append(arr, resp.BulkString(value))
	}
	return arr.WriteTo(ex.Buffer)
}

func hmset(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) <= 1 || len(v)%2 != 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "hmset").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	keyExists, tipe, expireAt := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := make(map[string][]byte)
	for i := 1; i < len(v); {
		hash[string(v[i])] = v[i+1]
		i += 2
	}
	ex.DB.PutHash(v[0], hash, expireAt)
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

func hset(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	keyExists, tipe, expireAt := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fieldExists := false

	hash := ex.DB.GetHashFields(v[0], [][]byte{v[1]})
	if hash[string(v[1])] != nil {
		fieldExists = true
	}

	hash[string(v[1])] = v[2]
	ex.DB.PutHash(v[0], hash, expireAt)

	if !fieldExists {
		return resp.OneInteger.WriteTo(ex.Buffer)
	}
	return resp.ZeroInteger.WriteTo(ex.Buffer)
}

func hsetnx(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	keyExists, tipe, expireAt := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fieldExists := false

	hash := ex.DB.GetHashFields(v[0], [][]byte{v[1]})
	if hash[string(v[1])] != nil {
		fieldExists = true
	}

	if !fieldExists {
		hash[string(v[1])] = v[2]
		ex.DB.PutHash(v[0], hash, expireAt)
		return resp.OneInteger.WriteTo(ex.Buffer)
	}
	return resp.ZeroInteger.WriteTo(ex.Buffer)
}

func hstrlen(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetHashFields(v[0], [][]byte{v[1]})
	return resp.Integer(len(hash[string(v[1])])).WriteTo(ex.Buffer)
}
