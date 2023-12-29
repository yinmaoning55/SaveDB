package main

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"strings"
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
	conns := make(map[net.Conn]Connection)
	tcpServer.Connections = conns
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
		go onMessage(&conn)
	}
}

type Connection struct {
	Conn   net.Conn
	Read   chan *Message
	Writer chan *Message
	Close  *atomic.Bool
	//连接的类型 1=client 2=集群中的其他实例 接收集群消息需要验证发送方的ip属不属于集群中的实例
	Type       byte
	RemoteAddr net.Addr
}
type OnConnection interface {
	ConnOpen()
	ConnClose()
	ReadMsg()
	WriterMsg()
}

func (c *Connection) ConnOpen() {
	SaveDBLogger.Infof("connection establishment conn=%v", c.Conn.RemoteAddr())
}
func (c *Connection) ConnClose() {
	delete(tcpServer.Connections, c.Conn)
	SaveDBLogger.Infof("connection closed conn=%v", c.Conn.RemoteAddr())
	_ = (c.Conn).Close()
}
func (c *Connection) ReadMsg() {
	defer func() {
		c.ConnClose()
	}()
	for {
		bufData := make([]byte, MSG_BUFFER_SIZE)
		buff1 := bufData[:MSG_BUFFER_OFFSET]
		_, err := io.ReadFull(c.Conn, buff1)
		if err != nil {
			SaveDBLogger.Errorf("Read data error %v, conn=%v", err, c.Conn.RemoteAddr())
			return
		}
		var mlen int32
		mlen = ReadInt(buff1)
		bufd := bufData[MSG_BUFFER_OFFSET : mlen+MSG_BUFFER_OFFSET]
		_, err = io.ReadFull(c.Conn, bufd)
		if err != nil {
			SaveDBLogger.Errorf("Read data error %v, conn=%v", err, c.Conn.RemoteAddr())
			return
		}
		//序列化指令格式为: command 参数1 参数2 参数3 ........
		str := string(bufd)
		words := strings.Fields(str)
		command := words[0]
		com, ok := saveCommandMap[command]
		if command == "heart" {
			SaveDBLogger.Infof("heart packet conn=%v", c.Conn.RemoteAddr())
			continue
		}
		//非法格式直接返回错误
		if !ok {
			msg := createWriterMsg(C_ERR)
			c.Writer <- msg
			continue
		}
		//非法参数长度直接返回错误
		if len(words)-1 > com.arity {
			msg := createWriterMsg(C_ERR)
			c.Writer <- msg
			continue
		}
		args := words[1:]
		msg := &Message{
			Conn:    &c.Conn,
			Command: &command,
			Args:    &args,
		}
		server.Read <- msg
	}
}
func (c *Connection) WriterMsg() {
	defer func() {
		if r := recover(); r != nil {
			SaveDBLogger.Errorf("WriterMsg from panic:%v, conn=%v", r, c.Conn.RemoteAddr())
		}
	}()
	for {
		select {
		case msg, ok := <-c.Writer:
			if !ok {
				//主动关闭链接了
				SaveDBLogger.Info("Connection close by self", c.Conn.RemoteAddr())
				if !c.Close.Load() {
					c.ConnClose()
				}
				return
			} else {
				data := msg.ReturnData
				_, _ = c.Conn.Write(*data)
			}
		default:
			break
		}
	}
}

type Message struct {
	Conn       *net.Conn
	Command    *string
	Args       *[]string
	ReturnData *[]byte
}

func onMessage(conn *net.Conn) {
	var connection = &Connection{}
	w := make(chan *Message)
	connection.Writer = w
	connection.Conn = *conn
	tcpServer.Connections[*conn] = *connection
	//先建立连接
	connection.ConnOpen()
	connection.RemoteAddr = (*conn).RemoteAddr()
	var flag atomic.Bool
	connection.Close = &flag
	//读写分离
	go connection.ReadMsg()
	connection.WriterMsg()
}

func createWriterMsg(str string) *Message {
	strBytes := []byte(str)
	// 创建一个带有长度前缀的字节数组
	data := make([]byte, 4+len(strBytes))
	// 将长度写入前四个字节
	binary.BigEndian.PutUint32(data[:4], uint32(len(strBytes)))
	copy(data[4:], strBytes)
	msg := &Message{ReturnData: &data}
	return msg
}
func createReadMsg() {

}
func ReadInt(bs []byte) int32 {
	u := binary.BigEndian.Uint32(bs)
	return int32(u)
}
func writeInt32(bs []byte, pos int, v int32) {
	binary.BigEndian.PutUint32(bs[pos:], uint32(v))
}
