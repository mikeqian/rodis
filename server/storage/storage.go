// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
    "fmt"
    "github.com/syndtr/goleveldb/leveldb/opt"
)

var storage [16]*LevelDB

func OpenStorage(dbPath string, options *opt.Options) error {
    for i := 0; i < 16; i ++ {
        d := dbPath + fmt.Sprintf("/%d", i)
        db, err := Open(d, options)
        if err != nil {
            return err
        }
        storage[i] = db
    }
    return nil
}

func SelectStorage(i int) *LevelDB {
    return storage[i]
}

func CloseStorage() {
    for _, ldb := range storage {
        ldb.Close()
    }
}
