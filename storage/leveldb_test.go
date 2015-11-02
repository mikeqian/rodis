// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"os"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
)

func TestPutGet(t *testing.T) {
	db, err := Open("testdb", nil)
	if err != nil {
		t.Fatalf("Open LevelDB error: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("testdb")
	}()

	db.Put([]byte("rod"), []byte("dong 1"))
	data, err := db.Get([]byte("rod"))
	if err != nil || string(data) != "dong 1" {
		t.Errorf("Put-Set rod 1 error, err=%v", err)
	}

	db.Put([]byte("rod"), []byte("dong 2"))
	data, err = db.Get([]byte("rod"))
	if err != nil || string(data) != "dong 2" {
		t.Errorf("Put-Set rod 2 error, err=%v", err)
	}
}

func TestDelete(t *testing.T) {
	db, err := Open("testdb", nil)
	if err != nil {
		t.Fatalf("Open LevelDB error: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("testdb")
	}()

	db.Put([]byte("rod"), []byte("dong"))
	err = db.Delete([]byte("rod"))
	if err != nil {
		t.Errorf("Delete error, err=%v", err)
	}

	_, err = db.Get([]byte("rod"))
	if err != leveldb.ErrNotFound {
		t.Errorf("Delete error, can get data.")
	}
}
