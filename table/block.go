package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/kelindar/bitmap"
	bolt "go.etcd.io/bbolt"
)

const DefaultBlockSize = 128 * 1024 * 1024

var (
	BucketBlocks = []byte("blocks")

	rawBlockPool          *sync.Pool
	errFoundAvailableSlot = errors.New("found available slot")
	errNotFoundBlock      = errors.New("not found block")
	errNoWriteable        = errors.New("no writeable")
)

func init() {
	rawBlockPool = &sync.Pool{New: func() any { return make([]byte, binary.Size(rawBlock{})) }}
}

type rawBlock struct {
	Id       uint64
	Size     uint64
	Filename string // full filename
	Refcnt   uint64
	Slots    bitmap.Bitmap // TODO
}

func marshalRawBlock(block *rawBlock) (b []byte, err error) {
	b = rawBlockPool.Get().([]byte)
	buf := bytes.NewBuffer(b)

	err = binary.Write(buf, binary.BigEndian, *block)
	if err != nil {
		rawBlockPool.Put(b)

		err = fmt.Errorf("marshal raw block error: %w", err)
		return
	}
	return
}

type Block struct {
	table *Table
	raw   rawBlock
}

func allocRawBlock(t *Table, tx *bolt.Tx) (rb *rawBlock, err error) {
	if !tx.Writable() {
		err = errNoWriteable
		return
	}

	bucket := tx.Bucket(BucketBlocks)
	id, err := bucket.NextSequence()
	if err != nil {
		return
	}

	bf, err := os.OpenFile(filepath.Join(t.blockDir, strconv.FormatUint(id, 10)),
		os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
	if err != nil {
		return
	}
	defer func(bf *os.File) {
		err := bf.Close()
		if err != nil {
			return
		}
	}(bf)

	var b = rawBlock{
		Id:       id,
		Size:     DefaultBlockSize,
		Filename: bf.Name(),
		Refcnt:   1,
	}

	k := Id2Bytes(id)
	v, err := marshalRawBlock(&b)
	if err != nil {
		return
	}
	defer func() {
		rawBlockPool.Put(k)
		rawBlockPool.Put(v)
	}()
	err = bucket.Put(k, v)
	if err != nil {
		return
	}
	rb = &b
	return
}

func freeRawBlock(t *Table, tx *bolt.Tx, id uint64) (err error) {
	if !tx.Writable() {
		err = errNoWriteable
		return
	}

	bucket := tx.Bucket(BucketBlocks)
	var k = Id2Bytes(id)
	defer PutId2Bytes(k)

	v := bucket.Get(k)
	if v == nil {
		err = fmt.Errorf("free raw block error: %w", errNotFoundBlock)
		return
	}
	var (
		rb  rawBlock
		buf = bytes.NewBuffer(v)
	)
	err = binary.Read(buf, binary.BigEndian, &rb)
	if err != nil {
		err = fmt.Errorf("free raw block error: %w", err)
		return
	}
	if rb.Refcnt <= 0 {
		// TODO, not allowed
	}
	rb.Refcnt -= 1
	buf.Reset()
	err = binary.Write(buf, binary.BigEndian, rb)
	if err != nil {
		err = fmt.Errorf("free raw block error: %w", err)
		return
	}
	err = bucket.Put(k, buf.Bytes())
	if err != nil {
		err = fmt.Errorf("free raw block error: %w", err)
		return
	}
	return
}

func findAvailableBlock(tx *bolt.Tx) (rb *rawBlock, slot uint64, err error) {
	if !tx.Writable() {
		err = errNoWriteable
		return
	}

	bucket := tx.Bucket(BucketBlocks)

	err = bucket.ForEach(func(k, v []byte) (err error) {
		var r *rawBlock
		buf := bytes.NewBuffer(v)

		err = binary.Read(buf, binary.BigEndian, &r)
		if err != nil {
			return
		}
		rs, ok := r.Slots.MinZero()
		if !ok {
			return
		}
		rb = r
		slot = uint64(rs) // XXX
		err = errFoundAvailableSlot
		return
	})
	if err != nil {
		if errors.Is(err, errFoundAvailableSlot) {
			err = nil
			return
		}
	}
	return
}

func setBlockSlot(tx *bolt.Tx, id uint64, slot uint64) (err error) {
	if !tx.Writable() {
		err = errNoWriteable
		return
	}

	bucket := tx.Bucket(BucketBlocks)
	var k = Id2Bytes(id)
	defer PutId2Bytes(k)

	v := bucket.Get(k)
	if v == nil {
		err = fmt.Errorf("set block slot error: %w", errNotFoundBlock)
		return
	}
	var (
		rb  rawBlock
		buf = bytes.NewBuffer(v)
	)
	err = binary.Read(buf, binary.BigEndian, &rb)
	if err != nil {
		err = fmt.Errorf("set block slot error: %w", err)
		return err
	}
	rb.Slots.Set(uint32(slot))
	buf.Reset()
	err = binary.Write(buf, binary.BigEndian, rb)
	if err != nil {
		err = fmt.Errorf("set block slot error: %w", err)
		return err
	}
	err = bucket.Put(k, buf.Bytes())
	if err != nil {
		err = fmt.Errorf("set block slot error: %w", err)
		return err
	}
	return
}

func addBlockRefcnt(tx *bolt.Tx, k []byte) (err error) {
	if !tx.Writable() {
		err = errNoWriteable
		return
	}

	bucket := tx.Bucket(BucketBlocks)

	v := bucket.Get(k)
	if v == nil {
		err = fmt.Errorf("add block refcnt error: %w", errNotFoundBlock)
		return
	}
	var (
		rb  rawBlock
		buf = bytes.NewBuffer(v)
	)
	err = binary.Read(buf, binary.BigEndian, &rb)
	if err != nil {
		err = fmt.Errorf("add block refcnt error: %w", err)
		return err
	}
	rb.Refcnt += 1
	buf.Reset()
	err = binary.Write(buf, binary.BigEndian, rb)
	if err != nil {
		err = fmt.Errorf("add block refcnt error: %w", err)
		return err
	}
	err = bucket.Put(k, buf.Bytes())
	if err != nil {
		err = fmt.Errorf("add block refcnt error: %w", err)
		return err
	}
	return
}
