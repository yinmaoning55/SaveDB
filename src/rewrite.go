package src

import "C"
import (
	"io"
	"os"
	"savedb/src/log"
	"strconv"
)

func (persister *Persister) newRewriteHandler() *Persister {
	h := &Persister{}
	h.db = persister.tmpDBMaker()
	return h
}

type RewriteCtx struct {
	tmpFile  *os.File // tmpFile is the file handler of aof tmpFile
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

func (persister *Persister) Rewrite() error {
	ctx, err := persister.StartRewrite()
	if err != nil {
		return err
	}
	err = persister.DoRewrite(ctx)
	if err != nil {
		return err
	}

	persister.FinishRewrite(ctx)
	return nil
}

func (persister *Persister) DoRewrite(ctx *RewriteCtx) (err error) {
	// start rewrite
	if !Config.AofUseRdbPreamble {
		log.SaveDBLogger.Info("generate aof preamble")
		err = persister.generateAof(ctx)
	} else {
		log.SaveDBLogger.Info("generate rdb preamble")
		err = persister.generateRDB(ctx)
	}
	return err
}

func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// pausing aof
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	err := persister.aofFile.Sync()
	if err != nil {
		log.SaveDBLogger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	fileInfo, _ := os.Stat(GetAofFilePath())
	filesize := fileInfo.Size()

	// create tmp file
	file, err := os.CreateTemp(Config.Dir, "*.aof")
	if err != nil {
		log.SaveDBLogger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		dbIdx:    persister.currentDB,
	}, nil
}

// 有互斥锁,线程安全
func (persister *Persister) FinishRewrite(ctx *RewriteCtx) {
	//加锁 和aof写操作互斥
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	tmpFile := ctx.tmpFile

	// copy commands executed during rewriting to tmpFile
	errOccurs := func() bool {
		/* read write commands executed during rewriting */
		src, err := os.Open(GetAofFilePath())
		if err != nil {
			log.SaveDBLogger.Error("open aofFilename failed: " + err.Error())
			return true
		}
		defer func() {
			_ = src.Close()
			_ = tmpFile.Close()
		}()
		//0：相对于文件的起始位置。 1：相对于当前文件指针位置。 2：相对于文件的末尾。
		_, err = src.Seek(ctx.fileSize, 0)
		if err != nil {
			log.SaveDBLogger.Error("seek failed: " + err.Error())
			return true
		}
		//文件尾部切换到当前的数据
		//data := ToBytes(ToCmdLine("select", strconv.Itoa(ctx.dbIdx)))
		//_, err = tmpFile.Write(data)
		//if err != nil {
		//	log.SaveDBLogger.Error("tmp file rewrite failed: " + err.Error())
		//	return true
		//}
		//把src复制到temp中 只复制重写期间产生的数据
		_, err = io.Copy(tmpFile, src)
		if err != nil {
			log.SaveDBLogger.Error("copy aof filed failed: " + err.Error())
			return true
		}
		return false
	}()
	if errOccurs {
		return
	}

	// replace current aof file by tmp file
	_ = persister.aofFile.Close()
	if err := os.Rename(tmpFile.Name(), GetAofFilePath()); err != nil {
		log.SaveDBLogger.Warn(err)
	}
	// 重新打开文件以便进一步写入
	aofFile, err := os.OpenFile(GetAofFilePath(), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	persister.aofFile = aofFile

	// write select command again to resume aof file selected db
	// it should have the same db index with  persister.currentDB
	data := ToBytes(ToCmdLine("select", strconv.Itoa(persister.currentDB)))
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
