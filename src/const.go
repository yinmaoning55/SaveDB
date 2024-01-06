package src

const (
	C_OK              = 1
	C_ERR             = 0
	OK_STR            = "OK"
	MSG_BUFFER_SIZE   = 65535
	MSG_BUFFER_OFFSET = 4
	Persistent        = 0
	TypeStr           = 1
	TypeHash          = 2
	TypeSet           = 3
	TypeZSet          = 4
	TypeList          = 5

	//和redis6.0一样
	ZSKIPLIST_MAXLEVEL = 32
	ZSKIPLIST_P        = 0.25
)
