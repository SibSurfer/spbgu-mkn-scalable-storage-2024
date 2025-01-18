package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func main() {
	b := [100]byte{}
	wg := sync.WaitGroup{}
	wg.Add(2)
	mtx := sync.Mutex{}
	cond := sync.NewCond(&mtx)
	go func() {
		for {
			filler(b[:len(b)/2], '0', '1')
			time.Sleep(time.Second)
			wg.Done()
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
		}
	}()
	go func() {
		for {
			filler(b[len(b)/2:], 'X', 'Y')
			time.Sleep(time.Second)
			wg.Done()
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
		}
	}()
	go func() {
		for {
			wg.Wait()
			fmt.Println(string(b[:]))
			time.Sleep(time.Second)
			wg.Add(2)
			cond.L.Lock()
			cond.Broadcast()
			cond.L.Unlock()
		}
	}()
	select {}
}
func filler(b []byte, ifzero byte, ifnot byte) {
	for i := 0; i < len(b); i++ {
		if rand.Intn(2) == 0 {
			b[i] = ifzero
		} else {
			b[i] = ifnot
		}
	}
}
