package table

import (
	"encoding/binary"
	"sync"
)

var idPool *sync.Pool

func init() {
	idPool = &sync.Pool{New: func() any { return make([]byte, 8) }}
}

func Id2Bytes(v uint64) (b []byte) {
	b = idPool.Get().([]byte)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func PutId2Bytes(b []byte) {
	idPool.Put(b)
}
