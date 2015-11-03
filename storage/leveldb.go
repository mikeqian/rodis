// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"errors"
	"sync"

	"github.com/rod6/log6"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	db  *leveldb.DB
	rwm *sync.RWMutex
}

var ErrLevelDB = errors.New("Backend Level DB Error")

func Open(dbPath string, options *opt.Options) (*LevelDB, error) {
	db, err := leveldb.OpenFile(dbPath, options)
	if err != nil {
		return nil, err
	}

	var rwmutex sync.RWMutex

	return &LevelDB{db: db, rwm: &rwmutex}, nil
}

func (ldb *LevelDB) Has(key []byte) (xkey []byte, exists bool, tipe byte, hasExpire bool, err error) {
	iter := ldb.db.NewIterator(util.BytesPrefix(key), nil)
	for iter.Next() {
		xkey = iter.Key()
		l := len(xkey)

		if l == len(key) + 1 {
			exists = true
			tipe = xkey[l-1] & 0x0F
			hasExpire = (xkey[l-1]&0xF0 == 0x10)
			break
		}
		xkey = nil
	}
	iter.Release()
	if erri := iter.Error(); erri != nil && erri != leveldb.ErrNotFound {
		log6.Error("LevelDB error: %v", erri)
		err = ErrLevelDB
	}
	return
}

func (ldb *LevelDB) Get(key []byte) (xkey []byte, value []byte, exists bool, tipe byte, hasExpire bool, err error) {
	iter := ldb.db.NewIterator(util.BytesPrefix(key), nil)
	for iter.Next() {
		xkey = iter.Key()
		l := len(xkey)

		if l == len(key) + 1 {
			exists = true
			value = iter.Value()
			tipe = xkey[l-1] & 0x0F
			hasExpire = (xkey[l-1]&0xF0 == 0x10)
			break
		}
		xkey = nil	// not found
	}
	iter.Release()
	if erri := iter.Error(); erri != nil && erri != leveldb.ErrNotFound {
		log6.Error("LevelDB error: %v", erri)
		err = ErrLevelDB
	}
	return
}

func (ldb *LevelDB) GetX(xkey []byte) ([]byte, error) {
	value, err := ldb.db.Get(xkey, nil)
	if err != nil && err != leveldb.ErrNotFound {
		log6.Error("LevelDB error: %v", err)
		err = ErrLevelDB
	}
	return value, err
}

func (ldb *LevelDB) Delete(key []byte) error {
	var xkey []byte

	iter := ldb.db.NewIterator(util.BytesPrefix(key), nil)
	for iter.Next() {
		xkey = iter.Key()
		if len(xkey) == len(key) + 1 {
			ldb.db.Delete(xkey, nil)
			break
		}
		xkey = nil
	}
	iter.Release()

	if err := iter.Error(); err != nil && err != leveldb.ErrNotFound {
		log6.Error("LevelDB error: %v", err)
		return ErrLevelDB
	}
	return nil
}

func (ldb *LevelDB) DeleteX(xkey []byte) (error) {
	if err := ldb.db.Delete(xkey, nil); err != nil && err != leveldb.ErrNotFound {
		log6.Error("LevelDB error: %v", err)
		return ErrLevelDB
	}
	return nil
}

func (ldb *LevelDB) Put(key []byte, value []byte, tipe byte, hasExpire bool) error {
	ldb.Delete(key)

	bite := tipe
	if hasExpire {
		bite |= 0x10
	}
	xkey := append(key, bite)
	if err := ldb.db.Put(xkey, value, nil); err != nil {
		log6.Error("LevelDB error: %v", err)
		return ErrLevelDB
	}
	return nil
}

func (ldb *LevelDB) PutX(xkey []byte, key []byte, value []byte, tipe byte, hasExpire bool) error {
	bite := tipe
	if hasExpire {
		bite |= 0x10
	}

	rkey := append(key, bite)
	if xkey == nil || xkey[len(xkey) - 1] == bite {	// same key & same type
		if err := ldb.db.Put(rkey, value, nil); err != nil {
			log6.Error("LevelDB error: %v", err)
			return ErrLevelDB
		}
		return nil
	}

	batch := new(leveldb.Batch)
	batch.Delete(xkey)
	batch.Put(rkey, value)
	if err := ldb.db.Write(batch, nil); err != nil && err != leveldb.ErrNotFound {
		log6.Error("LevelDB error: %v", err)
		return ErrLevelDB
	}
	return nil
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
