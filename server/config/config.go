// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package config

import (
	"github.com/BurntSushi/toml"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type RodisConfig struct {
	App     string
	Version float32
	Owner   string

	Listen      string
	RequirePass string

	LogLevel string

	LevelDBPath string
	LevelDB     *opt.Options
}

var Config RodisConfig

func LoadConfig(path string) error {
	if _, err := toml.DecodeFile(path, &Config); err != nil {
		return err
	}
	return nil
}
