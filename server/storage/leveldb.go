// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelDB struct {
    db *leveldb.DB
}

func Open(dbPath string, options *opt.Options) (*LevelDB, error) {
    db, err := leveldb.OpenFile(dbPath, options)
    if err != nil {
        return nil, err
    }

    return &LevelDB{db: db}, nil
}

func (ldb *LevelDB) Put(key []byte, value []byte) error {
    return ldb.db.Put(key, value, nil)
}

func (ldb *LevelDB) Get(key []byte) ([]byte, error) {
    return ldb.db.Get(key, nil)
}

func (ldb *LevelDB) Delete(key []byte) error {
    return ldb.db.Delete(key, nil)
}

func (ldb *LevelDB) Close() {
    if ldb.db != nil {
        ldb.db.Close()
    }
}
