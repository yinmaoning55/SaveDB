package timewheel

import (
	"time"
)

var tw = New(time.Second, 3600)

func init() {
	tw.Start()
}

func Delay(duration time.Duration, key string, job func()) {
	tw.AddJob(duration, key, job)
}

func AddTimer(at time.Time, key string, job func()) {
	tw.AddJob(at.Sub(time.Now()), key, job)
}

// Cancel stops a pending job
func Cancel(key string) {
	tw.RemoveJob(key)
}
