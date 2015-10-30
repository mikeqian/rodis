// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
    "strconv"
    "strings"

    "github.com/syndtr/goleveldb/leveldb"
    "github.com/rod6/rodis/server/resp"
)

// Sort the functions as the list on redis command page: http://redis.io/commands#string

// use appendx for append command, because append is a key word of golang
func appendx(v resp.CommandArgs, ex *CommandExtras) error {
    ex.DB.Lock()
    defer ex.DB.Unlock()

    val, err := ex.DB.Get(v[0])
    if err !=nil && err != leveldb.ErrNotFound {
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

func bitcount(v resp.CommandArgs, ex *CommandExtras) error {
    if len(v) != 1 && len(v) != 3 {
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
        sum += bitsCount[b]
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
            src, err := ex.DB.Get(b)
            if err != nil && err != leveldb.ErrNotFound {
                return err
            }
            if err == leveldb.ErrNotFound {
                continue
            }

            if len(destValue) < len(src) {
                if len(destValue) == 0 {    // loop first step
                    destValue = append(destValue, src...)
                    continue
                } else {
                    destValue = append(destValue, make([]byte, len(src) - len(destValue))...)
                }
            }
            for i, _ := range destValue {
                s := byte(0)
                if i < len(src) {
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

func getrange(v resp.CommandArgs, ex *CommandExtras) error {
    val, err := ex.DB.Get(v[0])
    if err != nil && err != leveldb.ErrNotFound {
        return err
    }

    if err == leveldb.ErrNotFound {
        return resp.Integer(0).WriteTo(ex.Buffer)
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
        return resp.EmptyBulkString.WriteTo(ex.Buffer)
    }

    return resp.BulkString(val[start:end]).WriteTo(ex.Buffer)
}

func set(v resp.CommandArgs, ex *CommandExtras) error {
    ex.DB.Lock()
    defer ex.DB.Unlock()

    if err := ex.DB.Put(v[0], v[1]); err != nil {
        return err
    }
    return resp.OkSimpleString.WriteTo(ex.Buffer)
}

func setnx(v resp.CommandArgs, ex *CommandExtras) error {
    ex.DB.Lock()
    defer ex.DB.Unlock()

    _, err := ex.DB.Get(v[0])
    if err != nil && err != leveldb.ErrNotFound {
        return err
    }
    if err == leveldb.ErrNotFound {
        if err := ex.DB.Put(v[0], v[1]); err != nil {
            return err
        }
        return resp.OneInteger.WriteTo(ex.Buffer)
    }
    return resp.ZeroInteger.WriteTo(ex.Buffer)
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

func getset(v resp.CommandArgs, ex *CommandExtras) error {
    ex.DB.Lock()
    defer ex.DB.Unlock()

    val, err_get := ex.DB.Get(v[0])
    if err_get !=nil && err_get != leveldb.ErrNotFound {
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

func incr(v resp.CommandArgs, ex *CommandExtras) error {
    ex.DB.Lock()
    defer ex.DB.Unlock()

    s, err := ex.DB.Get(v[0])
    if err != nil && err != leveldb.ErrNotFound {
        return err
    }
    val := int64(0)
    if err != nil && err == leveldb.ErrNotFound {
        val ++
    } else {
        val, err = strconv.ParseInt(string(s), 10, 64)
        if err != nil {
            return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
        }
        val ++
    }

    if err = ex.DB.Put(v[0], []byte(strconv.FormatInt(val, 10))); err != nil {
        return err
    }
    return resp.Integer(val).WriteTo(ex.Buffer)
}


func incrby(v resp.CommandArgs, ex *CommandExtras) error {
    ex.DB.Lock()
    defer ex.DB.Unlock()

    by, err := strconv.ParseInt(v[1].String(), 10, 64)
    if err != nil {
        return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
    }

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

// helper functions

var bitsCount = [256]int {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3,
  4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3,
  3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4,
  5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
  3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
  5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2,
  2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3,
  4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4,
  5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
  6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5,
  6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8}

func calcRange(start, end, len int) (int, int) {
    switch {
    case start >= len:
        start = len - 1
    case start <= -1 * len:
        start = 0
    case start < 0 && start > -1 * len:
        start = start + len
    }

    switch {
    case end >= len:
        end = len
    case end <= -1 * len:
        end = 1
    case end < 0 && start > -1 * len:
        end = len + end + 1
    default:
        end += 1
    }

    return start, end
}
