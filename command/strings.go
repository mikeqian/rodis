// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"strconv"
	"strings"

	"github.com/rod6/log6"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/rod6/rodis/resp"
)

// Implement for command list in http://redis.io/commands#string
//
// command		status		author		todo
// --------------------------------------------------
// APPEND		done        rod
// BITCOUNT		done		rod
// BITOP		done		rod
// BITPOS		done		rod
// DECR			done		rod
// DECRBY		done		rod
// GET			done		rod
// GETBIT		done		rod
// GETRANGE		done		rod
// GETSET		done		rod
// INCR			done		rod
// INCRBY		done		rod
// INCRBYFLOAT	done		rod
// MGET			done		rod
// MSET			done		rod
// MSETNX		done		rod
// PSETEX		pending				waiting for expire solution
// SET			partial		rod     EX/PX option, waiting for expire solution
// SETBIT		done		rod
// SETEX		pending				waiting for expire solution
// SETNX		done		rod
// SETRANGE		done		rod
// STRLEN		done		rod
//

// strings.basic group, including set, get, getrange, setrange, append, strlen, setnx, setxx, getset
func set(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) <= 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "set").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	if len(v) == 2 {
		if err := ex.DB.Put(v[0], v[1]); err != nil {
			return err
		}
		return resp.OkSimpleString.WriteTo(ex.Buffer)
	}

	option_nx := false
	option_xx := false
	expire_op := ""
	expire_val := int64(0)

	offset := 2
	for offset < len(v) {
		option := strings.ToLower(string(v[offset]))
		switch option {
		case "xx":
			option_xx = true
			offset++
		case "nx":
			option_nx = true
			offset++
		case "ex", "px":
			if offset == len(v)-1 { // no value more
				return resp.NewError(ErrFmtSyntax).WriteTo(ex.Buffer)
			}
			if i, err := strconv.ParseInt(string(v[offset+1]), 10, 64); err != nil {
				return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
			} else {
				expire_op = option
				expire_val = i
			}
			offset += 2
		default:
			return resp.NewError(ErrFmtSyntax).WriteTo(ex.Buffer)
		}
	}

	if option_nx && option_xx {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}

	exist := true
	if option_nx || option_xx {
		_, err := ex.DB.Get(v[0])
		if err == leveldb.ErrNotFound {
			exist = false
		}
	}

	if option_nx && exist {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	if option_xx && !exist {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}

	if err := ex.DB.Put(v[0], v[1]); err != nil {
		return err
	}

	// TODO: ex/px
	if false {
		log6.Info("Set expire: %v.%v", expire_op, expire_val)
	}
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

func get(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	val, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if err == leveldb.ErrNotFound {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	return resp.BulkString(val).WriteTo(ex.Buffer)
}

// use appendx for append command, because append is a key word of golang
func appendx(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	val, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}

	if err == leveldb.ErrNotFound {
		val = []byte("")
	}

	val = append(val, v[1]...)
	if err = ex.DB.Put(v[0], val); err != nil {
		return err
	}
	return resp.Integer(len(val)).WriteTo(ex.Buffer)
}

func getrange(v resp.CommandArgs, ex *CommandExtras) error {
	start, err := strconv.Atoi(string(v[1]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	end, err := strconv.Atoi(string(v[2]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	val, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if err == leveldb.ErrNotFound {
		return resp.EmptyBulkString.WriteTo(ex.Buffer)
	}

	start, end = calcRange(start, end, len(val))
	if end <= start {
		return resp.EmptyBulkString.WriteTo(ex.Buffer)
	}

	return resp.BulkString(val[start:end]).WriteTo(ex.Buffer)
}

func setrange(v resp.CommandArgs, ex *CommandExtras) error {
	i64, err := strconv.ParseInt(string(v[1]), 10, 32)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	offset := int(i64)
	if offset < 0 {
		return resp.NewError(ErrOffsetOutRange).WriteTo(ex.Buffer)
	}
	if offset+len(v[2]) > 536870912 { // 512M is the limit length
		return resp.NewError(ErrStringExccedLimit).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	val, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if len(val) < offset+len(v[2]) {
		val = append(val, make([]byte, len(v[2])+offset-len(val))...)
	}
	copy(val[offset:], v[2])

	if err = ex.DB.Put(v[0], val); err != nil {
		return err
	}
	return resp.Integer(len(val)).WriteTo(ex.Buffer)
}

func strlen(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	s, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}

	val := int64(0)
	if err == leveldb.ErrNotFound {
		val = 0
	} else {
		val = int64(len(s))
	}
	return resp.Integer(val).WriteTo(ex.Buffer)
}

func setnx(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	_, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if err == nil {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	if err := ex.DB.Put(v[0], v[1]); err != nil {
		return err
	}
	return resp.OneInteger.WriteTo(ex.Buffer)

}

func getset(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	val, err_get := ex.DB.Get(v[0])
	if err_get != nil && err_get != leveldb.ErrNotFound {
		return err_get
	}

	if err := ex.DB.Put(v[0], v[1]); err != nil {
		return err
	}

	if err_get != nil && err_get == leveldb.ErrNotFound {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	return resp.BulkString(val).WriteTo(ex.Buffer)
}

// strings.multi, includng mget, mset, msetnx

func mget(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) < 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "mget").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	arr := make(resp.Array, len(v))

	for i, g := range v {
		val, err := ex.DB.Get(g)

		if err != nil && err != leveldb.ErrNotFound {
			return err
		}
		if err == leveldb.ErrNotFound {
			arr[i] = resp.NilBulkString
		} else {
			arr[i] = resp.BulkString(val)
		}
	}

	return arr.WriteTo(ex.Buffer)
}

func mset(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) == 0 || len(v)%2 != 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "mset").WriteTo(ex.Buffer)
	}

	batch := new(leveldb.Batch)
	for i := 0; i < len(v); {
		batch.Put(v[i], v[i+1])
		i += 2
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	if err := ex.DB.WriteBatch(batch); err != nil {
		return err
	}
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

func msetnx(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) == 0 || len(v)%2 != 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "msetnx").WriteTo(ex.Buffer)
	}

	batch := new(leveldb.Batch)
	for i := 0; i < len(v); {
		batch.Put(v[i], v[i+1])
		i += 2
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for i := 0; i < len(v); {
		_, err := ex.DB.Get(v[i])
		if err != nil && err != leveldb.ErrNotFound {
			return err
		}
		if err != leveldb.ErrNotFound {
			return resp.ZeroInteger.WriteTo(ex.Buffer)
		}
		i += 2
	}

	if err := ex.DB.WriteBatch(batch); err != nil {
		return err
	}
	return resp.OneInteger.WriteTo(ex.Buffer)
}

// strings.bits, including getbit, bitcount, bitop, bitpos

func getbit(v resp.CommandArgs, ex *CommandExtras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	val, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if err == leveldb.ErrNotFound {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	offset, err := strconv.Atoi(string(v[1]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	if offset >= 8*len(val) {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	byten := offset / 8
	pos := offset % 8

	k := val[byten] >> uint32(7-pos) & 0x01
	return resp.Integer(k).WriteTo(ex.Buffer)
}

func setbit(v resp.CommandArgs, ex *CommandExtras) error {
	i64, err := strconv.ParseInt(string(v[1]), 10, 32)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	offset := uint32(i64)
	pos := offset % 8
	byten := offset / 8

	bit, err := strconv.Atoi(string(v[2]))
	if err != nil || bit != 0 && bit != 1 {
		return resp.NewError(ErrBitValueInvalid).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	val, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}

	if uint32(len(val)) < byten+1 {
		val = append(val, make([]byte, int(byten)+1-len(val))...)
	}

	k := val[byten] >> uint32(7-pos) & 0x01

	switch bit {
	case 0:
		clear := byte(^(0x01 << (7 - pos)))
		val[byten] = val[byten] & clear
	case 1:
		set := byte(0x01 << (7 - pos))
		val[byten] = val[byten] | set
	}
	if err := ex.DB.Put(v[0], val); err != nil {
		return err
	}
	return resp.Integer(k).WriteTo(ex.Buffer)
}

func bitcount(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) == 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "bitcount").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	val, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}

	if err == leveldb.ErrNotFound {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	if len(v) != 1 && len(v) != 3 {
		return resp.NewError(ErrFmtSyntax).WriteTo(ex.Buffer)
	}

	start := 0
	end := len(val)

	if len(v) == 3 {
		start, err = strconv.Atoi(string(v[1]))
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}

		end, err = strconv.Atoi(string(v[2]))
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}

		start, end = calcRange(start, end, len(val))
	}

	if end <= start {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	sum := 0
	for _, b := range val[start:end] {
		sum += countSetBits[b]
	}
	return resp.Integer(sum).WriteTo(ex.Buffer)
}

func bitop(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) < 3 {
		return resp.NewError(ErrFmtWrongNumberArgument, "bitop").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	op := strings.ToLower(string(v[0]))

	switch op {
	case "not":
		if len(v) > 3 {
			return resp.NewError(ErrBitOPNotError).WriteTo(ex.Buffer)
		}
		src, err := ex.DB.Get(v[2])
		if err != nil && err != leveldb.ErrNotFound {
			return err
		}
		if err == leveldb.ErrNotFound {
			return resp.ZeroInteger.WriteTo(ex.Buffer)
		}

		destValue := make([]byte, len(src))
		for i, b := range src {
			destValue[i] = ^b
		}
		if err := ex.DB.Put(v[1], destValue); err != nil {
			return err
		}
		return resp.Integer(len(destValue)).WriteTo(ex.Buffer)

	case "or", "and", "xor":
		var destValue []byte = nil
		for _, b := range v[2:] {
			found := true
			src, err := ex.DB.Get(b)
			if err != nil && err != leveldb.ErrNotFound {
				return err
			}
			if err == leveldb.ErrNotFound {
				found = false
			}

			if found && len(destValue) < len(src) {
				if len(destValue) == 0 { // loop first step
					destValue = append(destValue, src...)
					continue
				} else {
					destValue = append(destValue, make([]byte, len(src)-len(destValue))...)
				}
			}
			for i, _ := range destValue {
				s := byte(0)
				if found && i < len(src) {
					s = src[i]
				}
				switch op {
				case "or":
					destValue[i] |= s
				case "and":
					destValue[i] &= s
				case "xor":
					destValue[i] ^= s
				}
			}
		}
		if err := ex.DB.Put(v[1], destValue); err != nil {
			return err
		}
		return resp.Integer(len(destValue)).WriteTo(ex.Buffer)

	default:
		return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
	}
}

func bitpos(v resp.CommandArgs, ex *CommandExtras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "bitpos").WriteTo(ex.Buffer)
	}

	arg, err := strconv.Atoi(string(v[1]))
	if err != nil || arg != 0 && arg != 1 {
		return resp.NewError(ErrShouldBe0or1).WriteTo(ex.Buffer)
	}

	set := arg == 1   // set bit pos
	clear := arg == 0 // clear bit pos

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	val, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}

	// This is the same behavior as offical redis. Not sure why
	// not check the len(v) when key is missing
	if err == leveldb.ErrNotFound && set {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}
	if err == leveldb.ErrNotFound && clear {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	// Seam that: check the len(v) only when the key exists
	if len(v) > 4 {
		return resp.NewError(ErrFmtSyntax).WriteTo(ex.Buffer)
	}

	// Get the range.
	start := 0
	end := len(val)
	if len(v) >= 3 {
		start, err = strconv.Atoi(string(v[2]))
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}
	}
	if len(v) == 4 {
		end, err = strconv.Atoi(string(v[3]))
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}
	}
	start, end = calcRange(start, end, len(val))
	if end <= start {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}

	// Get the postion in the range
	pos := 0
	found := false
	for _, b := range val[start:end] {
		if clear && posFirstClear[b] != 8 {
			found = true
			pos += posFirstClear[b]
			break
		}
		if set && posFirstSet[b] != -1 {
			found = true
			pos += posFirstSet[b]
			break
		}
		pos += 8 // not found, pos += 1*byte
	}

	if found {
		return resp.Integer(8*start + pos).WriteTo(ex.Buffer)
	}

	// From http://redis.io/commands/bitpos
	// If we look for set bits (the bit argument is 1) and the string is
	// empty or composed of just zero bytes, -1 is returned.
	if !found && set {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}

	// If we look for clear bits (the bit argument is 0) and the string only
	// contains bit set to 1, the function returns the first bit not part of
	// the string on the right. So if the string is three bytes set to the
	// value 0xff the command BITPOS key 0 will return 24, since up to bit 23
	// all the bits are 1.
	// Basically, the function considers the right of the string as padded with
	// zeros if you look for clear bits and specify no range or the start argument
	// only.
	if !found && clear && len(v) < 4 { //len(v) < 4: no range 'end' specified
		return resp.Integer(8 * end).WriteTo(ex.Buffer)
	}
	// However, this behavior changes if you are looking for clear bits and
	// specify a range with both start and end. If no clear bit is found in
	// the specified range, the function returns -1 as the user specified a
	// clear range and there are no 0 bits in that range.
	if !found && clear && len(v) == 4 {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}
	return resp.NegativeOneInteger.WriteTo(ex.Buffer) // Should NEVER called
}

// strings.math, including incr, incrby, decr, decrby, decrfloat
func decr(v resp.CommandArgs, ex *CommandExtras) error {
	return incrdecrHelper(v, ex, -1)
}

func decrby(v resp.CommandArgs, ex *CommandExtras) error {
	by, err := strconv.ParseInt(v[1].String(), 10, 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	return incrdecrHelper(v, ex, by*-1)
}

func incr(v resp.CommandArgs, ex *CommandExtras) error {
	return incrdecrHelper(v, ex, 1)
}

func incrby(v resp.CommandArgs, ex *CommandExtras) error {
	by, err := strconv.ParseInt(v[1].String(), 10, 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	return incrdecrHelper(v, ex, by)
}

func incrbyfloat(v resp.CommandArgs, ex *CommandExtras) error {
	by, err := strconv.ParseFloat(v[1].String(), 64)
	if err != nil {
		return resp.NewError(ErrNotValidFloat).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	s, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	val := 0.0
	if err != nil && err == leveldb.ErrNotFound {
		val += by
	} else {
		val, err = strconv.ParseFloat(string(s), 64)
		if err != nil {
			return resp.NewError(ErrNotValidFloat).WriteTo(ex.Buffer)
		}
		val += by
	}

	s = []byte(strconv.FormatFloat(val, 'f', -1, 64))
	if err = ex.DB.Put(v[0], s); err != nil {
		return err
	}
	return resp.BulkString(s).WriteTo(ex.Buffer)
}

// strings.helper

var countSetBits = [256]int{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
}

var posFirstSet = [256]int{ // -1: no 1 in this byte
	-1, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
}

var posFirstClear = [256]int{ // -1: no 0 in this byte
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 7, 8,
}

func calcRange(start, end, len int) (int, int) {
	switch {
	case start >= len:
		start = len
	case start <= -1*len:
		start = 0
	case start < 0 && start > -1*len:
		start = start + len
	}

	switch {
	case end >= len:
		end = len
	case end <= -1*len:
		end = 1
	case end < 0 && start > -1*len:
		end = len + end + 1
	default:
		end += 1
	}

	return start, end
}

func incrdecrHelper(v resp.CommandArgs, ex *CommandExtras, by int64) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	s, err := ex.DB.Get(v[0])
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	val := int64(0)
	if err != nil && err == leveldb.ErrNotFound {
		val += by
	} else {
		val, err = strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}
		val += by
	}

	if err = ex.DB.Put(v[0], []byte(strconv.FormatInt(val, 10))); err != nil {
		return err
	}
	return resp.Integer(val).WriteTo(ex.Buffer)
}
