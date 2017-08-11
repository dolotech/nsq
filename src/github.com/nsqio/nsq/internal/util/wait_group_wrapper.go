package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}
// 同步执行一组函数
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
