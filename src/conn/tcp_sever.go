package conn

import (
	"context"
	"net"
	log "savedb/common"
	"strconv"
)

type TCPServer struct {
}

func StartTCPServer(port int) *TCPServer {
	server := &TCPServer{}
	address := ":" + strconv.Itoa(port)

	var lc net.ListenConfig
	var ctx context.Context
	listener, err := lc.Listen(ctx, "tcp", address)
	if err != nil {
		log.NetLogger.Error("TCP Server start fail, Listen :%s", address)
		return nil
	}
	log.NetLogger.Infof("TCP Server started, Listen :%s", address)

	go server.acceptConn(listener)

	return server
}
func (server *TCPServer) acceptConn(listener net.Listener) {
	defer func() {
		if r := recover(); r != nil {
			log.NetLogger.Errorf("AcceptConn  from panic:%v, recover again", r)
			go server.acceptConn(listener)
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.NetLogger.Error(err)
			continue
		}
		tcpConn := conn.(*net.TCPConn)
		err = tcpConn.SetNoDelay(true)
		if err != nil {
			log.NetLogger.Error("Error setting TCP NoDelay:", err)
		}

	}

}
