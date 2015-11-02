// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package net

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"runtime"

	"github.com/pborman/uuid"
	"github.com/rod6/log6"

	"github.com/rod6/rodis/command"
	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

type rodisConn struct {
	uuid   string
	db     *storage.LevelDB
	conn   net.Conn
	reader *bufio.Reader
	server *rodisServer
	buffer bytes.Buffer
	authed bool
	extras *command.CommandExtras
}

func newConnection(conn net.Conn, rs *rodisServer) {
	uuid := uuid.New()
	rc := &rodisConn{uuid: uuid, db: storage.SelectStorage(0), conn: conn, reader: bufio.NewReader(conn), server: rs}

	if rs.cfg.RequirePass == "" {
		rc.authed = true
	}

	rc.extras = &command.CommandExtras{rc.db, &rc.buffer, rc.authed, rs.cfg.RequirePass}

	rc.server.mu.Lock()
	rs.conns[uuid] = rc
	rc.server.mu.Unlock()

	log6.Debug("New connection: %v", uuid)

	go rc.handle()
}

func (rc *rodisConn) handle() {
	for {
		respType, respValue, err := resp.Parse(rc.reader)
		if err != nil {
			select {
			case <-rc.server.quit: // Server is quit, rc.close() is called.
				return
			default:
				break
			}

			if err == io.EOF { // Client close the connection
				log6.Debug("Client close connection %v.", rc.uuid)
				rc.close()
				return
			} else {
				log6.Warn("Connection %v error: %v", rc.uuid, err)
				continue // Other error, should continue the connection
			}
		}

		rc.response(respType, respValue)
	}
}

func (rc *rodisConn) response(respType resp.RESPType, respValue resp.Value) {
	defer func() {
		if err := recover(); err != nil {
			stack := make([]byte, 2048)
			stack = stack[:runtime.Stack(stack, false)]
			log6.Error("Panci in handling connection %v, command is %v, err is %s\n%s", rc.uuid, respValue, err, stack)
			rc.conn.Write([]byte("-ERR server unknown error\r\n"))
		}
	}()

	if respType != resp.ArrayType { // All command from client should be RESPArrayType
		log6.Error("Connection %v get a WRONG format command from client.", rc.uuid)
		rc.conn.Write([]byte("-ERR wrong input format\r\n"))
		return
	}

	err := command.Handle(respValue.(resp.Array), rc.extras)
	if err != nil {
		log6.Error("Connection %v get a server error: %v", rc.uuid, err)
		rc.conn.Write([]byte("-ERR server unknown error\r\n"))
		return
	}

	rc.conn.Write(rc.buffer.Bytes())
}

func (rc *rodisConn) close() {
	err := rc.conn.Close()
	if err != nil {
		log6.Debug("Connection %v close error: %v", rc.uuid, err)
	}

	rc.server.mu.Lock()
	delete(rc.server.conns, rc.uuid)
	rc.server.mu.Unlock()

	log6.Debug("Connection %v closed.", rc.uuid)
}
