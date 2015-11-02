// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelDB struct {
	db  *leveldb.DB
	rwm *sync.RWMutex
}

func Open(dbPath string, options *opt.Options) (*LevelDB, error) {
	db, err := leveldb.OpenFile(dbPath, options)
	if err != nil {
		return nil, err
	}

	var rwmutex sync.RWMutex

	return &LevelDB{db: db, rwm: &rwmutex}, nil
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

func (ldb *LevelDB) Flush() error {
	iter := ldb.db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		ldb.db.Delete(key, nil)
	}
	iter.Release()
	return iter.Error()
}

func (ldb *LevelDB) WriteBatch(batch *leveldb.Batch) error {
	return ldb.db.Write(batch, nil)
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
