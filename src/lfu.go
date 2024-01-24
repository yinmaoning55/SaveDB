package src

import (
	"math/rand"
	"runtime"
	"savedb/src/log"
	"time"
)

const (
	LfuInitVal                = 5
	ConfigDefaultLfuDecayTime = 1
	ConfigDefaultLfuLogFactor = 10
	EvpoolSize                = 16
	MaxmemorySamples          = 5
)

var EvictionPoolLRU []*evictionPoolEntry

type evictionPoolEntry struct {
	idle   uint64 //待淘汰的键值对的空闲时间
	key    string //待淘汰的键值对的key
	cached string //缓存的对象
	dbid   int    //待淘汰键值对的key所在的数据库ID
}

func (p *Persister) freeMemoryIfNeededAndSafe() int {
	if !p.loading.Load() {
		return freeMemoryIfNeeded()
	}
	return COk
}
func freeMemoryIfNeeded() int {
	Server.persister.pausingAof.Lock()
	defer Server.persister.pausingAof.Unlock()
	if Server.persister.usedMemorySize < Config.Maxmemory {
		return COk
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	//当前程序中所有堆分配的对象的总大小
	usedMemory := m.Alloc
	if usedMemory > Config.Maxmemory {
		mem_tofree := usedMemory - Config.Maxmemory
		log.SaveDBLogger.Warn("OutOfMemory, mem_tofree:%d, start cache lfu,release=", mem_tofree)
		for {
			usedMemory = m.Alloc
			if usedMemory < Config.Maxmemory {
				break
			}
			var pool = EvictionPoolLRU
			for i := 0; i < dbsSize; i++ {
				evictionPoolPopulate(i, &Server.FindDB(i).AllKeys, pool)
			}
			for k := EvpoolSize - 1; k >= 0; k-- {
				if pool[k].key == "" {
					continue
				}

				bestdbid := pool[k].dbid
				db := Server.FindDB(bestdbid)
				args := make([]string, 1)
				args = append(args, pool[k].key)

				/* Remove the entry from the pool. */
				Del(db, args)
				pool[k].key = ""
				pool[k].idle = 0
			}
			//TODO 手动GC一下?
			runtime.GC()
		}
	}
	return COk
}
func updateLFU(key *SaveObject) {
	counter := LFUDecrAndReturn(key)
	counter = LFULogIncr(counter)
	key.lru = (uint32(LFUGetTimeInMinutes() << 8)) | uint32(counter)
	key.refCount += 1
}

func LFUDecrAndReturn(key *SaveObject) uint8 {
	var ldt = uint16(key.lru >> 16)
	//取lru后8位
	var counter = uint8(key.lru & 255)
	var num_periods uint16 = 0
	//ConfigDefaultLfuDecayTime可以走配置 计算衰减大小
	if ConfigDefaultLfuDecayTime != 0 {
		num_periods = uint16(LFUTimeElapsed(uint64(ldt) / ConfigDefaultLfuDecayTime))
	}
	////如果衰减大小小于当前访问次数，那么，衰减后的访问次数是当前访问次数减去衰减大小；否则，衰减后的访问次数等于0
	if num_periods > 0 {
		if num_periods > uint16(counter) {
			counter = 0
		} else {
			counter -= uint8(num_periods)
		}
	}
	return counter
}

// 返回当前时间和某个对象上次访问时间的差值，是分钟
func LFUTimeElapsed(ldt uint64) uint64 {
	now := LFUGetTimeInMinutes()
	if now >= ldt {
		return now - ldt
	}
	return 65535 - ldt + now
}

// 返回当前时间，单位是分钟 取后16位
func LFUGetTimeInMinutes() uint64 {
	return uint64((time.Now().UnixMilli() / 1000 / 60) & 65535)
}

// 概率值 r 是随机定的，所以，阈值 p 的大小就决定了访问次数增加的难度。阈值 p 越小，概率值 r 小于 p 的可能性也越小，此时，访问次数也越难增加；
// 相反，如果阈值 p 越大，概率值 r 小于 p 的可能性就越大，访问次数就越容易增加
func LFULogIncr(counter uint8) uint8 {
	if counter == 255 {
		return 255
	}
	r := rand.Float64()
	baseval := float64(counter) - LfuInitVal
	if baseval < 0 {
		baseval = 0
	}
	//当计算阈值 p 时，我们是把 baseval 和 lfu-log-factor 乘积后，加上 1，然后再取其倒数。
	//所以，baseval 或者 lfu-log-factor 越大，那么其倒数就越小，也就是阈值 p 就越小；
	//反之，阈值 p 就越大
	p := 1.0 / (baseval*ConfigDefaultLfuLogFactor + 1)
	if r < p {
		counter++
	}
	return counter
}

func evictionPoolAlloc() {
	EvictionPoolLRU = make([]*evictionPoolEntry, EvpoolSize)
	for i := 0; i < EvpoolSize; i++ {
		e := &evictionPoolEntry{
			idle: 0,
			key:  "",
		}
		EvictionPoolLRU[i] = e
	}
}

func dictGetSomeKeys(d *AllKeys, des []*SaveObject, count int) int {
	stored := 0
	maxsteps := count * 10
	keysSize := d.keys.Len()
	if d.keys.Len() < count {
		count = d.keys.Len()
	}
	if keysSize <= 0 {
		return 0
	}
	step := keysSize / count
	min := 0
	max := step
	for stored < count && maxsteps > 0 {
		rand.Seed(time.Now().UnixNano())
		i := rand.Intn(max-min) + min
		maxsteps--
		if i > keysSize-1 {
			continue
		}
		v, _ := d.keys.GetAt(int(i))
		he := v.saveObj
		for he == nil {
			continue
		}
		des[stored] = he
		stored++
		if stored >= count {
			return stored
		}
		min += step
		max += step
	}
	return stored
}

func evictionPoolPopulate(dbid int, sampledict *AllKeys, pool []*evictionPoolEntry) {
	samples := make([]*SaveObject, MaxmemorySamples)
	count := dictGetSomeKeys(sampledict, samples, MaxmemorySamples)
	for i := 0; i < count; i++ {
		o := samples[i]
		idle := 255 - LFUDecrAndReturn(o)
		var k = 0
		for k < EvpoolSize &&
			pool[k].key != "" &&
			pool[k].idle < uint64(idle) {
			k++
		}
		if k == 0 && pool[EvpoolSize-1].key != "" {
			continue
		} else if k < EvpoolSize && pool[k].key == "" {
			/* Inserting into empty position. No setup needed before insert. */
		} else {
			if pool[EvpoolSize-1].key == "" {
				cached := pool[EvpoolSize-1].cached
				// 将元素向左移动一位
				copy(pool[k+1:], pool[k:EvpoolSize-1])
				pool[k].cached = cached
			} else {
				k--
				cached := pool[0].cached
				db := Server.FindDB(dbid)
				args := make([]string, 1)
				args = append(args, pool[0].key)
				Del(db, args)
				copy(pool, pool[1:k+1])
				pool[k].cached = cached
			}
		}
		pool[k].key = *samples[i].prt
		pool[k].idle = uint64(idle)
		pool[k].dbid = dbid
	}
}
