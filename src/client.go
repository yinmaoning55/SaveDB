package src

import (
	"encoding/binary"
	"encoding/json"
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
		go handleClientRead(conn, client)
		go handleClientWrite(&connection, conn)
	}()

	return client
}
func handleClientRead(con net.Conn, client *TCPClient) {
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
		buf1 := buf[:6]
		_, err := io.ReadFull(con, buf1)
		if err != nil {
			fmt.Println("time=  Read head error=", time.Now(), err)
			return
		}
		length := ReadInt(buf[2:6])
		_, err = io.ReadFull(con, buf1[6:6+length])
		if err != nil {
			fmt.Println("time=  Read head error=", time.Now(), err)
			return
		}
		msg := &Message{ReturnData: &buf}
		client.connection.Read <- msg
	}

}
func handleClientWrite(connection *Connection, conn net.Conn) {
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
				_, _ = conn.Write(*msg.ReturnData)
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

	for {
		select {
		case msg, ok := <-client.connection.Read:
			if !ok {
				//主动关闭链接了
				return "Connection close"
			} else {
				m := *msg.ReturnData
				status := Read2Byte(m[:2])
				len := ReadInt(m[2:6])
				restr := string(m[6 : 6+len])
				data := make(map[string]interface{})
				data["status"] = status
				data["msg"] = restr
				res, err := json.MarshalIndent(data, "", "\t")
				if err != nil {
					panic(err)
				}
				return string(res)
			}
		}
	}
}
func (client *TCPClient) GetConnection() Connection {
	return client.connection
}
func checkError(err error) {
	if err != nil {
		fmt.Println("Error:", err)
	}
}
