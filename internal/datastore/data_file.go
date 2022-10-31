package datastore

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/IslamWalid/bitcask/internal/recfmt"
	"github.com/IslamWalid/bitcask/internal/sio"
)

const maxFileSize = 10 * 1024

type DataFile struct {
    file        *sio.File
    fileName    string
    filePath    string
    fileFlags   int
    currentPos  int
    currentSize int
}

func (f *DataFile) Write(key, value string, tstamp uint64) (int, error) {
    rec := recfmt.Compress(key, value, tstamp)

    if len(rec) + f.currentSize > maxFileSize {
        err := f.newActiveFile()
        if err != nil {
            return 0, err
        }
    }

    n, err := f.file.Write(rec)
    if err != nil {
        return 0, err
    }

    writePos := f.currentPos
    f.currentPos += n
    f.currentSize += n

    return writePos, nil
}

func (f *DataFile) newActiveFile() error {
    err := f.file.Close()
    if err != nil {
        return err
    }
    
    fileName := fmt.Sprintf("%d.data", time.Now().UnixMicro())
    file, err := sio.OpenFile(path.Join(f.filePath, fileName), f.fileFlags, os.FileMode(0666))
    if err != nil {
        return err
    }

    f.file = file
    f.fileName = fileName
    f.currentPos = 0
    f.currentSize = 0

    return nil
}

func (f *DataFile) Read(pos, keySize, valueSize int) (*recfmt.DataRecord, error) {
    file, err := sio.Open(path.Join(f.filePath, f.fileName))
    if err != nil {
        return nil, err
    }
    defer file.Close()

    rec := make([]byte, keySize + valueSize + 14)

    _, err = file.ReadAt(rec, int64(pos))
    if err != nil {
        return nil, err
    }

    return recfmt.Extract(rec)
}
