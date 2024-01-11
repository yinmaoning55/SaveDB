package src

import (
	"bytes"
	"encoding/binary"
	"net"
	"strconv"
)

func CreateMsg(c *net.Conn, cmd string, args []string) *Message {
	msg := &Message{
		Conn:    c,
		Command: &cmd,
		Args:    args,
	}
	return msg
}
func CreateDefinedMsg(c *net.Conn, cmd string, arg ...string) *Message {
	args := make([]string, len(arg))
	for i, s := range arg {
		args[i] = s
	}
	msg := &Message{
		Conn:    c,
		Command: &cmd,
		Args:    args,
	}
	return msg
}
func StringToBytes(s string) []byte {
	return []byte(s)
}
func BytesToString(b []byte) string {
	return string(b)
}

type Result struct {
	Status byte
	Res    []byte
}

func CreateResult(status byte, res []byte) Result {
	if res == nil {
		res = make([]byte, 0)
	}
	return Result{Status: status, Res: res}
}
func CreateIntResult(status byte, i int64) Result {
	res := make([]byte, 0)
	writeInt64(res, 0, i)
	return Result{Status: status, Res: res}
}
func CreateStrResult(status byte, res string) Result {
	var b []byte
	if res == "" {
		b = make([]byte, 0)
	} else {
		b = []byte(res)
	}
	return Result{Status: status, Res: b}
}

func Equals(a interface{}, b interface{}) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}
func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}

func readFirstKey(args []string) ([]string, []string) {
	// assert len(args) > 0
	key := args[0]
	return []string{key}, nil
}

func writeFirstKey(args []string) ([]string, []string) {
	key := args[0]
	return nil, []string{key}
}

func writeAllKeys(args []string) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = v
	}
	return nil, keys
}

func readAllKeys(args []string) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = v
	}
	return keys, nil
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
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}
func ToBytes(args [][]byte) []byte {
	argLen := len(args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

func BytesArrayToStringArray(b [][]byte) []string {
	s := make([]string, len(b))
	for i, i2 := range b {
		s[i] = string(i2)
	}
	return s
}
