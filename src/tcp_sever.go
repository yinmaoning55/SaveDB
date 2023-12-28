package main

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
)

var tcpServer = &TCPServer{}

type TCPServer struct {
	Connections map[net.Conn]Connection
	Close       atomic.Bool
}

func StartTCPServer(port int) error {
	address := ":" + strconv.Itoa(port)
	var lc net.ListenConfig
	var ctx context.Context
	listener, err := lc.Listen(ctx, "tcp", address)
	if err != nil {
		SaveDBLogger.Error("TCP Server start fail, Listen :%s", address)
		return err
	}
	SaveDBLogger.Infof("TCP Server started, Listen :%s", address)
	go tcpServer.acceptConn(listener)
	return nil
}
func (server *TCPServer) acceptConn(listener net.Listener) {
	defer func() {
		if r := recover(); r != nil {
			SaveDBLogger.Errorf("AcceptConn  from panic:%v, recover again", r)
			go server.acceptConn(listener)
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			SaveDBLogger.Error(err)
			continue
		}
		tcpConn := conn.(*net.TCPConn)
		err = tcpConn.SetNoDelay(true)
		if err != nil {
			SaveDBLogger.Error("Error setting TCP NoDelay:", err)
		}
		//逻辑处理
	}
}

type Connection struct {
	Conn   *net.Conn
	Read   chan *Message
	Writer chan *Message
	Close  *atomic.Bool
}
type Message struct {
}
