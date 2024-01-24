package src

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
	_ "gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"net"
	"os"
	"runtime"
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
		bufData := make([]byte, MsgBufferSize)
		buff1 := bufData[:MsgBufferOffset]
		_, err := io.ReadFull(c.Conn, buff1)
		if err != nil {
			if err.Error() == io.EOF.Error() || errors.Is(err, io.ErrUnexpectedEOF) {
				log.SaveDBLogger.Infof("connection closed conn=%v", c.Conn.RemoteAddr())
				c.ConnClose()
			} else {
				log.SaveDBLogger.Errorf("Read data error %v, conn=%v", err, c.Conn.RemoteAddr())
			}
			return
		}

		var mlen int32
		mlen = ReadInt(buff1)
		bufd := bufData[MsgBufferOffset : mlen+MsgBufferOffset]
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
		command := strings.ToLower(words[0])
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
	msg := createWriterMsg(CreateStrResult(CErr, str))
	c.Writer <- msg
}
func CreateSpecialCMD(c *Connection, result Result, err error) {
	var wm *Message
	if err != nil {
		wm = createWriterMsg(CreateStrResult(CErr, err.Error()))
	} else {
		wm = createWriterMsg(result)
	}
	if c.Writer != nil {
		c.Writer <- wm
	}
}

var SConfig = &SentinelConfig{}
var Config = &serverConfig{}

type serverConfig struct {
	Port              int            `yaml:"port"`
	Appendfsync       string         `yaml:"appendfsync"`
	AofUseRdbPreamble bool           `yaml:"aof-use-rdb-preamble"`
	Dir               string         `yaml:"dir"`
	RDBFilename       string         `yaml:"rdbfilename"`
	AppendOnly        bool           `yaml:"appendonly"`
	AppendFilename    string         `yaml:"appendfilename"`
	Maxmemory         uint64         `yaml:"maxmemory"`
	Logs              *log.LogConfig `yaml:"logs"`
}

func (config *SentinelConfig) LoadSentinelConfig(path string) {
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

func (s *SaveServer) ForEche(index int, cb func(key string, entity any, expiration *time.Time) bool) {
	s.FindDB(index).ForEach(0, cb)
}
func (s *SaveServer) FindDB(index int) *SaveDBTables {
	return s.Dbs[index].Load().(*SaveDBTables)
}

func SelectDB(index int, conn *Connection) error {
	if index < 0 || index >= dbsSize {
		return fmt.Errorf("db index error")
	}
	conn.dbIndex = index
	return nil
}

var CronManager *cron.Cron

func InitServer() {
	//Initialize the LRU keys pool
	evictionPoolAlloc()
	NewSingleServer()
	CronManager = cron.New(cron.WithSeconds())
	CronManager.Start()
	_, _ = CronManager.AddFunc("@every 5s", printMemoryStats)
}

// 单机下启动
func NewSingleServer() {
	err := os.MkdirAll(Config.Dir, os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("create tmp dir failed: %v", err))
	}
	validAof := false
	//1.先判断是否开启aof
	//2.如果开启就判断是否存在rdb文件，使用rdb+aof的混合模式恢复数据
	if Config.AppendOnly {
		validAof = fileExists(GetAofFilePath())
	}
	aofHandler, err := NewPersister2(*Server, Config.Appendfsync)
	if err != nil {
		panic(err)
	}
	if Config.AppendOnly {
		//todo 启动时暂时只重放aof文件
		aofHandler.LoadAof(0)
		//打开文件时的标志位，使用位掩码
		//os.O_APPEND: 将文件指针设置为文件末尾，在文件中追加数据。 os.O_CREATE: 如果文件不存在，则创建文件。 os.O_RDWR: 以读写方式打开文件。
		aofFile, err := os.OpenFile(GetAofFilePath(), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			panic(err)
		}
		aofHandler.aofFile = aofFile
		aofHandler.aofChan = make(chan *payload, aofQueueSize)
		aofHandler.aofFinished = make(chan struct{})
	}
	Server.bindPersister(aofHandler)
	//3.如果aof文件不存在则加载rdb
	if Config.RDBFilename != "" && !validAof {
		// load rdb
		err := Server.loadRdbFile()
		if err != nil {
			log.SaveDBLogger.Errorf("load rdb err: %v", err)
		}
	}
}

// 打印堆内存使用情况
func printMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	//当前程序中所有堆分配的对象的总大小
	m1 := m.Alloc / 1024 / 1024
	log.SaveDBLogger.Infof("heap monery: %v MiB", m1)
	if Config.Maxmemory > 0 {
		Server.persister.usedMemorySize = m.Alloc
	}
	//从启动开始已经分配的总内存量。这个值包括已经释放的内存，以及仍然被使用的内存
	log.SaveDBLogger.Infof("TotalAlloc: %v MiB", m.TotalAlloc/1024/1024)
	//程序在运行时分配的所有内存，包括堆、栈和其他运行时使用的内存
	s := m.Sys / 1024 / 1024
	log.SaveDBLogger.Infof("Sys: %v MiB", s)
	log.SaveDBLogger.Infof("GC num: %v", m.NumGC)
}

func GetRDBFilePath() string {
	return Config.Dir + "/" + Config.RDBFilename
}
func GetAofFilePath() string {
	return Config.Dir + "/" + Config.AppendFilename
}
