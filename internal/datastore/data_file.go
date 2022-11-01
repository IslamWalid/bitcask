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

type ActiveFile struct {
    fileWrapper *sio.File
    fileName    string
    filePath    string
    fileFlags   int
    currentPos  int
    currentSize int
}

func (a *ActiveFile) Write(key, value string, tstamp int64) (int, error) {
    rec := recfmt.CompressDataFileRec(key, value, tstamp)

    if a.fileWrapper == nil || len(rec) + a.currentSize > maxFileSize {
        err := a.newActiveFile()
        if err != nil {
            return 0, err
        }
    }

    n, err := a.fileWrapper.Write(rec)
    if err != nil {
        return 0, err
    }

    writePos := a.currentPos
    a.currentPos += n
    a.currentSize += n

    return writePos, nil
}

func (a *ActiveFile) newActiveFile() error {
    err := a.fileWrapper.File.Close()
    if err != nil {
        return err
    }
    
    fileName := fmt.Sprintf("%d.data", time.Now().UnixMicro())
    file, err := sio.OpenFile(path.Join(a.filePath, fileName), a.fileFlags, os.FileMode(0666))
    if err != nil {
        return err
    }

    a.fileWrapper = file
    a.fileName = fileName
    a.currentPos = 0
    a.currentSize = 0

    return nil
}

func (a *ActiveFile) Read(pos, keySize, valueSize int) (*recfmt.DataRec, error) {
    file, err := sio.Open(path.Join(a.filePath, a.fileName))
    if err != nil {
        return nil, err
    }
    defer file.File.Close()

    rec := make([]byte, keySize + valueSize + 14)

    _, err = file.ReadAt(rec, int64(pos))
    if err != nil {
        return nil, err
    }

    dataRec, _, err := recfmt.ExtractDataFileRec(rec)
    if err != nil {
        return nil, err
    }

    return dataRec, nil
}

func (a *ActiveFile) Name() string {
    return a.fileName
}

func (a *ActiveFile) Sync() error {
    return a.fileWrapper.File.Sync()
}

func (a *ActiveFile) Close() {
    a.fileWrapper.File.Close()
}
