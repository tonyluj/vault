package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

type Table struct {
	db       *bolt.DB // metadata db
	blockDir string   // dir to store blocks
}

var (
	ErrNoAvailableDB = errors.New("no available db")

	BucketFiles = []byte("files")
)

func (t *Table) OpenTable(db string) (table *Table, err error) {
	table = new(Table)
	table.db, err = bolt.Open(db, 0600, nil)
	if err != nil {
		err = fmt.Errorf("open table error: %w", err)
		return
	}
	err = table.db.Update(func(tx *bolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(BucketBlocks)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(BucketFiles)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(BucketPages)
		if err != nil {
			return err
		}
		return
	})
	if err != nil {
		err = fmt.Errorf("open table error: %w", err)
		return
	}
	return
}
func (t *Table) OpenFile(filename []byte) (f File, err error) {
	if t.db == nil {
		err = fmt.Errorf("open file error: %w", ErrNoAvailableDB)
		return
	}

	err = t.db.View(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket(BucketFiles)
		v := b.Get(filename)
		if v == nil {
			return
		}
		buf := bytes.NewBuffer(v)
		err = binary.Read(buf, binary.BigEndian, &f.raw)
		if err != nil {
			return
		}
		return
	})
	if err != nil {
		err = fmt.Errorf("open file error: %w", err)
		return
	}

	return
}

func (t *Table) List(prefix []byte) (files []File, err error) {
	if t.db == nil {
		err = fmt.Errorf("open file error: %w", ErrNoAvailableDB)
		return
	}

	files = make([]File, 0, 16)
	err = t.db.View(func(tx *bolt.Tx) (err error) {
		c := tx.Bucket(BucketFiles).Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var file File

			buf := bytes.NewBuffer(v)
			err = binary.Read(buf, binary.BigEndian, &file.raw)
			if err != nil {
				return
			}
			files = append(files, file)
		}
		return
	})
	if err != nil {
		err = fmt.Errorf("list error: %w", err)
		return
	}
	return
}
