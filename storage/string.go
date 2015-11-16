package storage

import (
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

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
