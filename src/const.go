package src

const (
	COk             = 1
	CErr            = 0
	OkStr           = "OK"
	MsgBufferSize   = 65535
	MsgBufferOffset = 4
	Persistent      = 0
	TypeStr         = 1
	TypeHash        = 2
	TypeSet         = 3
	TypeZSet        = 4
	TypeList        = 5

	//和redis6.0一样
	ZskiplistMaxlevel = 32
	ZskiplistP        = 0.25
	CRLF              = "\r\n"
)

type EmptyMultiBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

// MakeEmptyMultiBulkReply creates EmptyMultiBulkReply
func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

var emptyMultiBulkBytes = []byte("*0\r\n")

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

type NullBulkReply struct{}

func (n NullBulkReply) ToBytes() []byte {
	//TODO implement me
	panic("implement me")
}
