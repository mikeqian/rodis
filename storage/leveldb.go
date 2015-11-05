// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
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

func (ldb *LevelDB) DeleteString(key []byte) {
	metaKey := encodeMetaKey(key)
	valueKey := encodeStringKey(key)

	ldb.delete([][]byte{metaKey, valueKey})
}

func (ldb *LevelDB) GetString(key []byte) []byte {
	valueKey := encodeStringKey(key)
	return ldb.get(valueKey)
}

func (ldb *LevelDB) PutString(key []byte, value []byte, expireAt *time.Time) {
	metaKey := encodeMetaKey(key)
	valueKey := encodeStringKey(key)

	exists, tipe, _ := ldb.has(metaKey)
	if exists && tipe != String { // If exists data is not string, should delete it.
		ldb.delete([][]byte{metaKey, valueKey})
	}

	batch := new(leveldb.Batch)
	batch.Put(metaKey, encodeMetadata(String, expireAt))
	batch.Put(valueKey, value)
	if err := ldb.db.Write(batch, nil); err != nil {
		panic(err)
	}
}

func (ldb *LevelDB) DeleteHash(key []byte) {
	keys := [][]byte{encodeMetaKey(key)}

	// enum fields, and delete all
	hashPrefix := encodeHashFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	for iter.Next() {
		key := append([]byte{}, iter.Key()...)
		keys = append(keys, key)
	}
	iter.Release()
	ldb.delete(keys)
}

func (ldb *LevelDB) DeleteHashFields(key []byte, fields [][]byte) {
	// Delete fields
	keys := make([][]byte, len(fields))
	for i, field := range fields {
		keys[i] = encodeHashFieldKey(key, field)
	}
	ldb.delete(keys)

	// After delete, remove the hash meta entry if no fields in this hash
	hashPrefix := encodeHashFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	if !iter.Next() {
		ldb.delete([][]byte{encodeMetaKey(key)}) // No field, delete the hash
	}
	iter.Release()
}

func (ldb *LevelDB) GetHash(key []byte) map[string][]byte {
	hash := make(map[string][]byte)

	hashPrefix := encodeHashFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	for iter.Next() {
		// Find the seperator '|'
		sepIndex := strings.IndexByte(string(iter.Key()), '|')
		// The field name should be the string after '|'
		key := append([]byte{}, iter.Key()[sepIndex+1:]...)
		value := append([]byte{}, iter.Value()...)
		hash[string(key)] = value
	}
	iter.Release()
	return hash
}

func (ldb *LevelDB) GetHashFieldNames(key []byte) [][]byte {
	fields := [][]byte{}

	hashPrefix := encodeHashFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	for iter.Next() {
		// Find the seperator '|'
		sepIndex := strings.IndexByte(string(iter.Key()), '|')
		// The field name should be the string after '|'
		key := append([]byte{}, iter.Key()[sepIndex+1:]...)
		fields = append(fields, key)
	}
	iter.Release()
	return fields
}

func (ldb *LevelDB) GetHashFields(key []byte, fields [][]byte) map[string][]byte {
	hash := make(map[string][]byte)
	for _, field := range fields {
		fieldValue := ldb.get(encodeHashFieldKey(key, field))
		hash[string(field)] = fieldValue
	}
	return hash
}

func (ldb *LevelDB) PutHash(key []byte, hash map[string][]byte, expireAt *time.Time) {
	metaKey := encodeMetaKey(key)

	batch := new(leveldb.Batch)
	batch.Put(metaKey, encodeMetadata(Hash, expireAt))
	for k, v := range hash {
		fieldKey := encodeHashFieldKey(key, []byte(k))
		batch.Put(fieldKey, v)
	}
	if err := ldb.db.Write(batch, nil); err != nil {
		panic(err)
	}
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
