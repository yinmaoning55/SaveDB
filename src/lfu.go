package src

import (
	"math/rand"
	"time"
)

const (
	LfuInitVal                = 5
	ConfigDefaultLfuDecayTime = 1
	ConfigDefaultLfuLogFactor = 10
)

func freeMemoryIfNeededAndSafe() int {
	if !Server.persister.loading.Load() {
		return freeMemoryIfNeeded()
	}
	return COk
}
func freeMemoryIfNeeded() int {
	return COk
}
func updateLFU(key *SaveObject) {
	counter := LFUDecrAndReturn(key)
	counter = LFULogIncr(counter)
	key.lru = (uint32(LFUGetTimeInMinutes() << 8)) | uint32(counter)
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
