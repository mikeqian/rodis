// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Because leveldb/rocksdb is a PURE key/value data engine, to store complex redis data type
// is not that simple.
// In Rodis, the solution is useing two+ kev/value entries for one redis key/value: one for meta
// data, others for value data. String data only one data entries, set/hash may have more than
// one value data entries.
// To explain the format, I use rKey (redis key) to represent the key from redis client, metaKey
// for the metadata key in leveldb/rocksdb, valueKey for the value data key.
//
// Metakey always has a prefix: '+', metaKey := '+' + rKey
// Metadata format:
//      first byte: meta data version
//      second byte: lower 4 bits: RedisType, upper 4 bits: if has expire value
//      3rd - 18the: the time binary, represent the expire time.
// Valuekey always has a prefix: '-'. For hash/set data type, use seperator '|' to seperate the
// valuekey and inner element key.
//      String Type: '-' + rKey
//      Hash/Set/SortedSet: '-' + rKey + '|' + elementKey
//
package storage
