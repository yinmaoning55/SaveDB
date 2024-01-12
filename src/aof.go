package src

import (
	"context"
	"io"
	"os"
	"savedb/src/log"
	"strconv"
	"strings"
	"sync"
	"time"

	rdb "github.com/hdt3213/rdb/core"
)

type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 20
)

const (
	//  do fsync for every command
	FsyncAlways = "always"
	//  do fsync every second
	FsyncEverySec = "everysec"
	//  lets operating system decides when to do fsync
	FsyncNo = "no"
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

func NewPersister(db SaveServer, filename string, load bool, fsync string, tmpDBMaker func() SaveServer) (*Persister, error) {
	persister := &Persister{}
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	// load aof file if needed
	if load {
		persister.LoadAof(0)
	}
	//打开文件时的标志位，使用位掩码
	//os.O_APPEND: 将文件指针设置为文件末尾，在文件中追加数据。
	//os.O_CREATE: 如果文件不存在，则创建文件。
	//os.O_RDWR: 以读写方式打开文件。
	aofFile, err := os.OpenFile(GetAofFilePath(), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	// start aof goroutine to write aof file in background and fsync periodically if needed (see fsyncEverySecond)
	go func() {
		persister.listenCmd()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	// fsync every second if needed
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// aofChan will be set as nil temporarily during load aof see Persister.LoadAof
	if persister.aofChan == nil {
		return
	}

	//always同步保存
	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}

	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}

}

func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

func (persister *Persister) writeAof(p *payload) {
	persister.buffer = persister.buffer[:0] // reuse underlying array
	persister.pausingAof.Lock()             // prevent other goroutines from pausing aof
	defer persister.pausingAof.Unlock()
	// ensure aof is in the right database
	if p.dbIndex != persister.currentDB {
		// select db
		selectCmd := ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			log.SaveDBLogger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}
	// save command
	data := MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		log.SaveDBLogger.Warn(err)
	}
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

func (persister *Persister) LoadAof(maxBytes int) {
	// persister.db.Exec may call persister.AddAof
	// delete aofChan to prevent loaded commands back into aofChan
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(GetAofFilePath())
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		log.SaveDBLogger.Warn(err)
		return
	}
	defer file.Close()

	// load rdb preamble if needed
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// no rdb preamble
		//offset 设置为 0，表示不进行偏移，而 whence 设置为 io.SeekStart，表示将文件指针设置到文件的起始位置。
		_, _ = file.Seek(0, io.SeekStart)
	} else {
		// has rdb preamble
		//设置文件从哪里开始读
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		//上次落盘到现在产生的aof日志
		maxBytes = maxBytes - decoder.GetReadCount()
	}
	var reader io.Reader
	if maxBytes > 0 {
		//只读取刚才aof同步落盘时的数据
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	//异步加载aof文件 通过channel传递
	ch := ParseStream(reader)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			log.SaveDBLogger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			log.SaveDBLogger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*MultiBulkReply)
		if !ok {
			log.SaveDBLogger.Error("require multi bulk protocol")
			continue
		}
		s := BytesArrayToStringArray(r.Args)
		msg := CreateMsg(nil, s[0], s[1:])
		//插入数据库
		persister.db.Exec(nil, msg)
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

func (persister *Persister) Fsync() {
	persister.pausingAof.Lock()
	if err := persister.aofFile.Sync(); err != nil {
		log.SaveDBLogger.Errorf("fsync failed: %v", err)
	}
	persister.pausingAof.Unlock()
}

// Close gracefully stops aof persistence procedure
func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished // wait for aof finished
		err := persister.aofFile.Close()
		if err != nil {
			log.SaveDBLogger.Warn(err)
		}
	}
	persister.cancel()
}

// 1秒执行一次落盘
func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.Fsync()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

func (persister *Persister) generateAof(ctx *RewriteCtx) error {
	// rewrite aof tmpFile
	tmpFile := ctx.tmpFile
	// load aof tmpFile
	tmpAof := persister.newRewriteHandler()
	tmpAof.LoadAof(int(ctx.fileSize))
	for i := 0; i < dbsSize; i++ {
		// select db
		data := MakeMultiBulkReply(ToCmdLine("select", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		// dump db
		tmpAof.db.ForEche(i, func(key string, entity any, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}
