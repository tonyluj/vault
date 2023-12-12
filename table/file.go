package table

import (
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

const DefaultPageSize = 4096

type rawFile struct {
	Id   uint64
	Size uint64
	Name string
}

type File struct {
	table  *Table
	raw    rawFile
	name   []byte
	offset uint64
}

var (
	errNotFoundFile = errors.New("not found file")
)

func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (f *File) Close() error {
	//TODO implement me
	panic("implement me")
}

func (f *File) Read(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (f *File) Write(p []byte) (n int, err error) {
	err = f.table.db.Update(func(tx *bolt.Tx) (err error) {
		bucket := tx.Bucket(BucketFiles)
		v := bucket.Get(f.name)
		if v == nil {
			err = fmt.Errorf("write error: %w", errNotFoundFile)
			return
		}

		return
	})
	return
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	//TODO implement me
	panic("implement me")
}
