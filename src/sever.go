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
	"os"
	"savedb/src/log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var TcpServer = &TCPServer{}

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
		log.SaveDBLogger.Error("TCP Server start fail, Listen :%s", address)
		return err
	}
	log.SaveDBLogger.Infof("TCP Server started, Listen :%s", address)
	conns := make(map[net.Conn]Connection)
	TcpServer.Connections = conns
	go TcpServer.acceptConn(listener)
	return nil
}
func (server *TCPServer) acceptConn(listener net.Listener) {
	defer func() {
		if r := recover(); r != nil {
			log.SaveDBLogger.Errorf("AcceptConn  from panic:%v, recover again", r)
			go server.acceptConn(listener)
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.SaveDBLogger.Error(err)
			continue
		}
		tcpConn := conn.(*net.TCPConn)
		err = tcpConn.SetNoDelay(true)
		if err != nil {
			log.SaveDBLogger.Error("Error setting TCP NoDelay:", err)
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
	dbIndex    int
}
type OnConnection interface {
	ConnOpen()
	ConnClose()
	ReadMsg()
	WriterMsg()
}

func (c *Connection) ConnOpen() {
	log.SaveDBLogger.Infof("connection establishment conn=%v", c.Conn.RemoteAddr())
}

func (c *Connection) ConnClose() {
	delete(TcpServer.Connections, c.Conn)
	log.SaveDBLogger.Infof("connection closed conn=%v", c.Conn.RemoteAddr())
	_ = (c.Conn).Close()
}

func (c *Connection) ReadMsg() {
	defer func() {
		if r := recover(); r != nil {
			log.SaveDBLogger.Errorf("read error from panic:%v", r)
		}
		c.ConnClose()
	}()
	for {
		bufData := make([]byte, MSG_BUFFER_SIZE)
		buff1 := bufData[:MSG_BUFFER_OFFSET]
		_, err := io.ReadFull(c.Conn, buff1)
		if err != nil {
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				log.SaveDBLogger.Infof("client close err %v, conn=%v", err, c.Conn.RemoteAddr())
			} else {
				log.SaveDBLogger.Errorf("Read data error %v, conn=%v", err, c.Conn.RemoteAddr())
			}
			return
		}

		var mlen int32
		mlen = ReadInt(buff1)
		bufd := bufData[MSG_BUFFER_OFFSET : mlen+MSG_BUFFER_OFFSET]
		_, err = io.ReadFull(c.Conn, bufd)
		if err != nil {
			log.SaveDBLogger.Errorf("Read data error %v, conn=%v", err, c.Conn.RemoteAddr())
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
			log.SaveDBLogger.Infof("heart packet conn=%v", c.Conn.RemoteAddr())
			continue
		}

		//非法格式直接返回错误
		if !ok {
			ReturnErr("command error", c)
			continue
		}
		//非法参数长度直接返回错误
		if com.arity > 0 && len(words)-1 != com.arity {
			ReturnErr("command error", c)
			continue
		}
		args := words[1:]
		msg := CreateMsg(&c.Conn, command, args)
		Server.Exec(c, msg)
	}
}

func (c *Connection) WriterMsg() {
	defer func() {
		if r := recover(); r != nil {
			log.SaveDBLogger.Errorf("WriterMsg from panic:%v, conn=%v", r, c.Conn.RemoteAddr())
		}
	}()
	for {
		select {
		case msg, ok := <-c.Writer:
			if !ok {
				//主动关闭链接了
				log.SaveDBLogger.Info("Connection close by self", c.Conn.RemoteAddr())
				if !c.Close.Load() {
					c.ConnClose()
				}
				return
			} else {
				data := msg.ReturnData
				if c.Conn == nil {
					continue
				}
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
	defer func() {
		if r := recover(); r != nil {
			log.SaveDBLogger.Errorf("onMessage from panic:%v, conn=%v", r, (*conn).RemoteAddr())
		}
	}()
	var connection = &Connection{}
	w := make(chan *Message)
	connection.Writer = w
	connection.Conn = *conn
	var flag atomic.Bool
	connection.Close = &flag
	//默认0号数据库
	connection.dbIndex = 0
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

var Config = &serverConfig{}

type serverConfig struct {
	Port              int            `yaml:"port"`
	Appendfsync       string         `yaml:"appendfsync"`
	AofUseRdbPreamble bool           `yaml:"aof-use-rdb-preamble"`
	Dir               string         `yaml:"dir"`
	RDBFilename       string         `yaml:"rdbfilename"`
	AppendOnly        bool           `yaml:"appendonly"`
	AppendFilename    string         `yaml:"appendfilename"`
	Logs              *log.LogConfig `yaml:"logs"`
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

var Server = &SaveServer{}

type SaveServer struct {
	Dbs       []*atomic.Value
	persister *Persister
}

func (s SaveServer) ForEche(index int, cb func(key string, entity any, expiration *time.Time) bool) {
	FindDB(index).ForEach(0, cb)
}
func FindDB(index int) *SaveDBTables {
	return Server.Dbs[index].Load().(*SaveDBTables)
}

func SelectDB(index int, conn *Connection) error {
	if index < 0 || index >= dbsSize {
		return fmt.Errorf("db index error")
	}
	conn.dbIndex = index
	return nil
}

func InitServer() {
	NewSingleServer()
}

// 单机下启动
func NewSingleServer() {
	err := os.MkdirAll(Config.Dir, os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("create tmp dir failed: %v", err))
	}
	validAof := false
	//先判断是否开启aof
	if Config.AppendOnly {
		validAof = fileExists(GetAofFilePath())
	}
	aofHandler, err := NewPersister2(*Server, true, Config.Appendfsync)
	if err != nil {
		panic(err)
	}
	Server.bindPersister(aofHandler)
	//如果aof文件不存在则加载rdb
	if Config.RDBFilename != "" && !validAof {
		// load rdb
		err := Server.loadRdbFile()
		if err != nil {
			log.SaveDBLogger.Errorf("load rdb err: %v", err)
		}
	}
}
func GetRDBFilePath() string {
	return Config.Dir + "/" + Config.RDBFilename
}
func GetAofFilePath() string {
	return Config.Dir + "/" + Config.AppendFilename
}
