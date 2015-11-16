// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"errors"
	"time"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelDB struct {
	db  *leveldb.DB
	rwm *sync.RWMutex
}

const STRBYTE byte = 0x00

var ErrLevelDB = errors.New("Backend Level DB Error")
var ErrNotFound = leveldb.ErrNotFound

func Open(dbPath string, options *opt.Options) (*LevelDB, error) {
	db, err := leveldb.OpenFile(dbPath, options)
	if err != nil {
		return nil, err
	}

	var rwmutex sync.RWMutex

	return &LevelDB{db: db, rwm: &rwmutex}, nil
}

func (ldb *LevelDB) Has(key []byte) (bool, byte, *time.Time) {
	metaKey := encodeMetaKey(key)
	return ldb.has(metaKey)
}

func (ldb *LevelDB) has(metaKey []byte) (bool, byte, *time.Time) {
	metadata, err := ldb.db.Get(metaKey, nil)

	if err != nil && err != leveldb.ErrNotFound {
		panic(err)
	}

	if err == leveldb.ErrNotFound {
		return false, None, nil
	}

	tipe, expireAt, err := parseMetadata(metadata)
	if err != nil {
		panic(err)
	}
	return true, tipe, expireAt
}

func (ldb *LevelDB) delete(keys [][]byte) {
	batch := new(leveldb.Batch)
	for _, key := range keys {
		batch.Delete(key)
	}
	if err := ldb.db.Write(batch, nil); err != nil && err != leveldb.ErrNotFound {
		panic(err)
	}
}

func (ldb *LevelDB) get(key []byte) []byte {
	value, err := ldb.db.Get(key, nil)
	if err != nil && err != ErrNotFound {
		panic(err)
	}
	return value
}

func (ldb *LevelDB) Flush() error {
	iter := ldb.db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		ldb.db.Delete(key, nil)
	}
	iter.Release()
	return iter.Error()
}

func (ldb *LevelDB) Close() {
	if ldb.db != nil {
		ldb.db.Close()
	}
}

func (ldb *LevelDB) RLock() {
	ldb.rwm.RLock()
}

func (ldb *LevelDB) RUnlock() {
	ldb.rwm.RUnlock()
}

func (ldb *LevelDB) Lock() {
	ldb.rwm.Lock()
}

func (ldb *LevelDB) Unlock() {
	ldb.rwm.Unlock()
}
