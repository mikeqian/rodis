// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package main

import (
    "flag"
    "os"
    "os/signal"
    "syscall"

    "github.com/rod6/log6"

    "github.com/rod6/rodis/server/config"
    "github.com/rod6/rodis/server/net"
)

func main() {
    configFile := flag.String("c", "rodis.toml", "Rodis config file path")
    flag.Parse()

    if err := config.LoadConfig(*configFile); err != nil {
        log6.Fatal("Load/Parse config file error: %v", err)
    }

    log6.ParseLevel(config.Config.LogLevel)

    rs, err := net.NewServer(config.Config)
    if err != nil {
        log6.Fatal("New server error: %v", err)
    }

    defer rs.Close()

    sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go rs.Run()

	<-sc
}
