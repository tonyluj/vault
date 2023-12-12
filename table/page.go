package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	bolt "go.etcd.io/bbolt"
)

type rawPage struct {
	Id        uint64
	FileId    uint64
	BlockId   uint64
	BlockSlot uint64
	Offset    uint64
	Size      uint64
	Refcnt    uint64
}

type Page struct {
	table *Table
	raw   rawPage
	block *Block
}

var (
	BucketPages = []byte("pages")

	errFoundAvailablePages = errors.New("found available pages")

	rawPagePool *sync.Pool
)

func init() {
	rawPagePool = &sync.Pool{New: func() any { return make([]byte, binary.Size(rawPage{})) }}
}

func findAvailablePages(t *Table, tx *bolt.Tx, n uint64) (pages []*Page, err error) {
	if !tx.Writable() {
		err = errNoWriteable
		return
	}

	var (
		left   = n
		bucket = tx.Bucket(BucketPages)
	)

	pages = make([]*Page, 0, n)
	err = bucket.ForEach(func(k, v []byte) (err error) {
		if left == 0 {
			err = errFoundAvailablePages
			return
		}

		var (
			rp  rawPage
			buf = bytes.NewBuffer(v)
		)

		er := binary.Read(buf, binary.BigEndian, &rp)
		if er != nil {
			// TODO
			return
		}
		// reuse
		if rp.Refcnt != 0 {
			return
		}
		rp.Refcnt += 1
		buf.Reset()
		er = binary.Write(buf, binary.BigEndian, rp)
		if er != nil {
			// TODO
			return
		}
		er = bucket.Put(k, buf.Bytes())
		if er != nil {
			// TODO
			return
		}

		var page = Page{
			table: t,
			raw:   rp,
			block: &Block{
				table: t,
			},
		}

		bp := tx.Bucket(BucketBlocks)
		bk := Id2Bytes(rp.BlockId)
		bv := bp.Get(bk)
		if bv == nil {
			PutId2Bytes(bk)
			// TODO
		}
		PutId2Bytes(bk)
		bvBuf := bytes.NewBuffer(bv)
		er = binary.Read(bvBuf, binary.BigEndian, &page.block.raw)
		if er != nil {
			// TODO
			return
		}

		pages = append(pages, &page)
		left -= 1
		return
	})
	if err != nil {
		if !errors.Is(err, errFoundAvailablePages) {
			err = fmt.Errorf("find available pages error: %w", err)
			return
		}
		err = nil
	}
	ps, err := allocPages(t, tx, left)
	if err != nil {
		err = fmt.Errorf("find available pages error: %w", err)
		return
	}
	pages = append(pages, ps...)

	return
}

func freeRawPages(t *Table, tx *bolt.Tx, ids []uint64) (err error) {
	if !tx.Writable() {
		err = errNoWriteable
		return
	}

	bucket := tx.Bucket(BucketPages)

	for _, id := range ids {
		k := Id2Bytes(id)

		v := bucket.Get(k)
		if v == nil {
			// TODO
			PutId2Bytes(k)
			continue
		}
		PutId2Bytes(k)

		var (
			rp  rawPage
			buf = bytes.NewBuffer(v)
		)
		er := binary.Read(buf, binary.BigEndian, &rp)
		if er != nil {
			// TODO
			continue
		}
		if rp.Refcnt <= 0 {
			// TODO
		}
		rp.Refcnt -= 1
		rp.FileId = 0
		buf.Reset()
		er = binary.Write(buf, binary.BigEndian, rp)
		if er != nil {
			continue
		}
		er = bucket.Put(k, buf.Bytes())
		if er != nil {
			continue
		}
	}
	return
}

func allocPages(t *Table, tx *bolt.Tx, n uint64) (pages []*Page, err error) {
	if n < 1 {
		return
	}
	if !tx.Writable() {
		err = errNoWriteable
		return
	}

	pages = make([]*Page, 0, n)
	bucket := tx.Bucket(BucketPages)

	var i uint64
	for i = 0; i < n; i++ {
		// find available block
		var (
			rb      *rawBlock
			slot    uint64
			created bool
		)
		rb, slot, _ = findAvailableBlock(tx)
		if rb == nil {
			rb, err = allocRawBlock(t, tx)
			if err != nil {
				// TODO
				err = fmt.Errorf("alloc pages error: %w", err)
				return
			}
			created = true
		}
		id, err := bucket.NextSequence()
		if err != nil {
			if created {
				er := freeRawBlock(t, tx, rb.Id)
				if er != nil {
					// TODO: log
				}
			}
			err = fmt.Errorf("alloc pages error: %w", err)
			return
		}

		var page = Page{
			table: t,
			raw: rawPage{
				Id:        id,
				BlockId:   rb.Id,
				BlockSlot: slot,
				Offset:    0,
				Size:      DefaultPageSize,
				Refcnt:    1,
			},
			block: &Block{
				table: t,
				raw:   *rb,
			},
		}
		err = setBlockSlot(tx, page.raw.Id, slot)
		if err != nil {
			if created {
				er := freeRawBlock(t, tx, rb.Id)
				if er != nil {
					// TODO: log
				}
			}
			err = fmt.Errorf("alloc pages error: %w", err)
			return
		}

		pages = append(pages, &page)
	}
	return
}
