package sio

import (
	"io/fs"
	"os"
)

type File struct {
    file *os.File
}

func OpenFile(name string, flag int, perm fs.FileMode) (*File, error) {
    file, err := os.OpenFile(name, flag, perm)
    if err != nil {
        return nil, err
    }

    f := &File{
    	file: file,
    }

    return f, nil
}

func Open(name string) (*File, error) {
    file, err := os.Open(name)
    if err != nil {
        return nil, err
    }

    f := &File{
    	file: file,
    }

    return f, nil
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
    attempts := 0
    n, err := f.file.ReadAt(b, off)
    for i := n; err != nil; i += n {
        if attempts == 5 {
            return 0, err
        }
        off += int64(i)
        n, err = f.file.ReadAt(b[i:], int64(off))
    }

    return len(b), nil
}

func (f *File) Write(b []byte) (int, error) {
    n, err := f.file.Write(b)

    attempts := 0
    for i := n; err != nil; i += n {
        if attempts == 5 {
            return 0, err
        }
        n, err = f.file.Write(b[i:])
        attempts++
    }

    return len(b), nil
}

func (f *File) Close() error {
    return f.file.Close()
}
