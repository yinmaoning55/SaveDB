package src

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
)

var TcpServer = &TCPServer{}

type TCPServer struct {
	Connections map[net.Conn]Connection
	Close       atomic.Bool
}

var saveCommandMap map[string]saveDBCommand

// 所有的命令 基本上和redis一样
type saveDBCommand struct {
	name            string                                       //参数名字
	saveCommandProc func(db *saveDBTables, args []string) Result //执行的函数
	arity           int                                          //参数个数
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
	TcpServer.Connections = conns
	allRead := make(chan *Message)
	server.Read = allRead
	go TcpServer.acceptConn(listener)
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
	//连接的类型 1=client 2=集群中的其他实例 接收集群消息需要验证发送方的ip属不属于集群中的实例
	Type       byte
	Close      *atomic.Bool
	Conn       net.Conn
	Read       chan *Message
	Writer     chan *Message
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
	delete(TcpServer.Connections, c.Conn)
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
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				SaveDBLogger.Infof("client close err %v, conn=%v", err, c.Conn.RemoteAddr())
			} else {
				SaveDBLogger.Errorf("Read data error %v, conn=%v", err, c.Conn.RemoteAddr())
			}
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
		if str == "" {
			ReturnErr("command is null", c)
			continue
		}
		words := strings.Fields(str)
		command := words[0]
		com, ok := saveCommandMap[command]
		if command == "heart" {
			SaveDBLogger.Infof("heart packet conn=%v", c.Conn.RemoteAddr())
			continue
		}
		//非法格式直接返回错误
		if !ok {
			ReturnErr("command error", c)
			continue
		}
		//非法参数长度直接返回错误
		if len(words)-1 < com.arity {
			ReturnErr("command error", c)
			continue
		}
		args := words[1:]
		msg := &Message{
			Conn:    &c.Conn,
			Command: &command,
			Args:    args,
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
		}
	}
}

type Message struct {
	Conn       *net.Conn
	Command    *string
	Args       []string
	ReturnData *[]byte
}

func onMessage(conn *net.Conn) {
	var connection = &Connection{}
	w := make(chan *Message)
	connection.Writer = w
	connection.Conn = *conn
	var flag atomic.Bool
	connection.Close = &flag
	TcpServer.Connections[*conn] = *connection
	//先建立连接
	connection.ConnOpen()
	connection.RemoteAddr = (*conn).RemoteAddr()

	//读写分离
	go connection.ReadMsg()
	connection.WriterMsg()
}

func createWriterMsg(res Result) *Message {
	// 创建一个带有长度前缀的字节数组
	data := make([]byte, 2+4+len(res.Res))
	// 将长度写入前四个字节
	binary.BigEndian.PutUint16(data[:2], uint16(res.Status))
	binary.BigEndian.PutUint32(data[2:6], uint32(len(res.Res)))
	copy(data[6:], res.Res)
	msg := &Message{ReturnData: &data}
	return msg
}
func ReturnErr(str string, c *Connection) {
	msg := createWriterMsg(CreateStrResult(C_ERR, str))
	c.Writer <- msg
}
func createReadMsg() {

}

var Config *serverConfig = &serverConfig{}

type serverConfig struct {
	Port int        `yaml:"port"`
	Logs *LogConfig `yaml:"logs"`
}

func (config *serverConfig) LoadConfig(path string) {
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println("Open config file error", err.Error())
		return
	}
	e := yaml.Unmarshal(yamlFile, config)
	if e != nil {
		fmt.Println("read config file error", err.Error())
		return
	}
	config.Logs.DefaultLevel = "info"
}

var server = &saveServer{}

type saveServer struct {
	Read chan *Message
}

func ReadInt(bs []byte) int32 {
	u := binary.BigEndian.Uint32(bs)
	return int32(u)
}
func Read2Byte(bs []byte) int16 {
	u := binary.BigEndian.Uint16(bs)
	return int16(u)
}
func writeInt32(bs []byte, pos int, v int32) {
	binary.BigEndian.PutUint32(bs[pos:], uint32(v))
}
func writeInt64(bs []byte, pos int, v int64) {
	binary.BigEndian.PutUint64(bs[pos:], uint64(v))
}
func MainGoroutine() {
	defer func() {
		if e := recover(); e != nil {
			SaveDBLogger.Errorf("recover the panic:", e)
		}
	}()
	for {
		select {
		case msg, ok := <-server.Read:
			conn := TcpServer.Connections[*msg.Conn]
			if !ok {
				if conn.Close.Load() {
					conn.ConnClose()
				}
			} else {
				//逻辑处理
				command := saveCommandMap[*msg.Command]
				//写回
				wMsg := createWriterMsg(command.saveCommandProc(db, msg.Args))
				TcpServer.Connections[*msg.Conn].Writer <- wMsg
			}
		}
	}

}
