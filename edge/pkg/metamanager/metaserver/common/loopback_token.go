package common

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
)

const LoopbackUser = "system:edgecore-loopback"

var (
	loopbackOnce  sync.Once
	loopbackToken string
)

func LoopbackToken() string {
	loopbackOnce.Do(func() {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			panic(err)
		}
		loopbackToken = hex.EncodeToString(b)
	})
	return loopbackToken
}
