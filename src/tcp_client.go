package src

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

type TCPClient struct {
	connection Connection
}

func (client *TCPClient) close() {
	client.connection.Conn.Close()
}
func StartClient(ip string, port int) *TCPClient {
	client := &TCPClient{}
	address := ip + ":" + strconv.Itoa(port)
	recvMsgChan := make(chan *Message, 20)
	sendMsgChan := make(chan *Message, 20)
	var flag atomic.Bool
	connection := Connection{
		Read:   recvMsgChan,
		Writer: sendMsgChan,
		Close:  &flag,
	}
	client.connection = connection
	go func() {
		conn, err := net.Dial("tcp", address)
		checkError(err)
		fmt.Println(conn.LocalAddr().String(), "<->", address, " conn successful.")
		client.connection.Conn = conn
		go handleClienRead(conn, client)
		go handleClienWrite(&connection, conn)
	}()

	return client
}
func handleClienRead(con net.Conn, client *TCPClient) {
	defer func() {
		if !client.connection.Close.Load() {
			client.connection.Close.Store(true)
			client.close()
			close(client.connection.Writer)
			close(client.connection.Read)
		}
	}()
	for {
		buf := make([]byte, 65535)
		buff1 := buf[:MSG_BUFFER_OFFSET]
		_, err := io.ReadFull(con, buff1)
		if err != nil {
			fmt.Println("time=  Read head error=", time.Now(), err)
			return
		}

		var mlen int32
		mlen = ReadInt(buf[:MSG_BUFFER_OFFSET])
		bufd := buf[MSG_BUFFER_OFFSET : mlen+MSG_BUFFER_OFFSET]
		_, err = io.ReadFull(con, bufd)
		if err != nil {
			fmt.Println("Read data error")
			return
		}
		msg := &Message{ReturnData: &bufd}
		client.connection.Read <- msg
	}

}
func handleClienWrite(connection *Connection, conn net.Conn) {
	defer func() {
		if !connection.Close.Load() {
			connection.Close.Store(true)
			close(connection.Read)
		}
	}()
	for {
		select {
		case msg, ok := <-connection.Writer:
			if !ok {
				//主动关闭链接了
				fmt.Println("Connection close")
				return
			} else {
				conn.Write(*msg.ReturnData)
			}
		}
	}
}
func (client *TCPClient) SendMsg(str string) string {
	strBytes := []byte(str)
	// 创建一个带有长度前缀的字节数组
	data := make([]byte, 4+len(strBytes))
	// 将长度写入前四个字节
	binary.BigEndian.PutUint32(data[:4], uint32(len(strBytes)))
	copy(data[4:], strBytes)
	msg := &Message{ReturnData: &data}
	client.connection.Writer <- msg
	defer func() {
		client.close()
		if !client.connection.Close.Load() {
			client.connection.Close.Store(true)
			close(client.connection.Read)
		}
	}()
	restr := ""
	for {
		select {
		case msg, ok := <-client.connection.Read:
			if !ok {
				//主动关闭链接了
				fmt.Println("Connection close by self", client.connection.RemoteAddr)
				return "Connection close"
			} else {
				restr = string(*msg.ReturnData)
				return restr
			}
		}
	}
	return ""
}
func (c *TCPClient) GetConnection() Connection {
	return c.connection
}
func checkError(err error) {
	if err != nil {
		fmt.Println("Error:", err)
	}
}
