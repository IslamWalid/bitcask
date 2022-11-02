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

const (
	Merge  AppendType = 0
	Active AppendType = 1
)

type AppendType int

type AppendFile struct {
	fileWrapper *sio.File
	hintWrapper *sio.File
	fileName    string
	filePath    string
	fileFlags   int
	appendType  AppendType
	currentPos  int
	currentSize int
}

func (a *AppendFile) WriteData(key, value string, tstamp int64) (int, error) {
	rec := recfmt.CompressDataFileRec(key, value, tstamp)

	if a.fileWrapper == nil || len(rec)+a.currentSize > maxFileSize {
		err := a.newAppendFile()
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

func (a *AppendFile) WriteHint(key string, rec recfmt.KeyDirRec) error {
	buf := recfmt.CompressHintFileRec(key, rec)
	_, err := a.hintWrapper.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func (a *AppendFile) newAppendFile() error {
	if a.fileWrapper != nil {
		err := a.fileWrapper.File.Close()
		if err != nil {
			return err
		}
		if a.appendType == Merge {
			err := a.hintWrapper.File.Close()
			if err != nil {
				return err
			}
		}
	}

	tstamp := time.Now().UnixMicro()
	fileName := fmt.Sprintf("%d.data", tstamp)
	file, err := sio.OpenFile(path.Join(a.filePath, fileName), a.fileFlags, os.FileMode(0666))
	if err != nil {
		return err
	}

	if a.appendType == Merge {
		hintName := fmt.Sprintf("%d.hint", tstamp)
		hint, err := sio.OpenFile(path.Join(a.filePath, hintName), a.fileFlags, os.FileMode(0666))
		if err != nil {
			return err
		}
		a.hintWrapper = hint
	}

	a.fileWrapper = file
	a.fileName = fileName
	a.currentPos = 0
	a.currentSize = 0

	return nil
}

func (a *AppendFile) Name() string {
	return a.fileName
}

func (a *AppendFile) Sync() error {
	if a.fileWrapper != nil {
		return a.fileWrapper.File.Sync()
	}
	return nil
}

func (a *AppendFile) Close() {
	if a.fileWrapper != nil {
		a.fileWrapper.File.Close()
		if a.appendType == Merge {
			a.hintWrapper.File.Close()
		}
	}
}
