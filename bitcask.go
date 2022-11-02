package bitcask

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IslamWalid/bitcask/internal/datastore"
	"github.com/IslamWalid/bitcask/internal/keydir"
	"github.com/IslamWalid/bitcask/internal/recfmt"
)

const (
	ReadOnly     ConfigOpt = 0
	ReadWrite    ConfigOpt = 1
	SyncOnPut    ConfigOpt = 2
	SyncOnDemand ConfigOpt = 3
)

const requireWrite = "require write permission"

type ConfigOpt int

type options struct {
	syncOption       ConfigOpt
	accessPermission ConfigOpt
}

type Bitcask struct {
	keyDir     keydir.KeyDir
	usrOpts    options
	keyDirMu   sync.Mutex
	readerCnt  int32
	dataStore  *datastore.DataStore
	activeFile *datastore.AppendFile
	fileFlags  int
}

func Open(dataStorePath string, opts ...ConfigOpt) (*Bitcask, error) {
	b := &Bitcask{}
	b.usrOpts = parseUsrOpts(opts)

	var privacy keydir.KeyDirPrivacy
	var lockMode datastore.LockMode

	if b.usrOpts.accessPermission == ReadWrite {
		privacy = keydir.PrivateKeyDir
		lockMode = datastore.ExclusiveLock
		fileFlags := os.O_CREATE | os.O_RDWR
		if b.usrOpts.syncOption == SyncOnPut {
			fileFlags |= os.O_SYNC
		}
		b.fileFlags = fileFlags
		b.activeFile = datastore.NewAppendFile(dataStorePath, b.fileFlags, datastore.Active)
	} else {
		privacy = keydir.SharedKeyDir
		lockMode = datastore.SharedLock
	}

	dataStore, err := datastore.NewDataStore(dataStorePath, lockMode)
	if err != nil {
		return nil, err
	}

	keyDir, err := keydir.New(dataStorePath, privacy)
	if err != nil {
		return nil, err
	}

	b.dataStore = dataStore
	b.keyDir = keyDir

	return b, nil
}

func (b *Bitcask) Get(key string) (string, error) {
	if b.readerCnt == 0 {
		b.keyDirMu.Lock()
	}
	atomic.AddInt32(&b.readerCnt, 1)

	rec, isExist := b.keyDir[key]

	atomic.AddInt32(&b.readerCnt, -1)
	if b.readerCnt == 0 {
		b.keyDirMu.Unlock()
	}

	if !isExist {
		return "", errors.New(fmt.Sprintf("%s: %s", key, datastore.KeyNotExist))
	}

	return b.dataStore.ReadValueFromFile(rec.FileId, key, rec.ValuePos, rec.ValueSize)
}

func (b *Bitcask) Put(key, value string) error {
	if b.usrOpts.accessPermission == ReadOnly {
		return errors.New(fmt.Sprintf("Put: %s", requireWrite))
	}

	tstamp := time.Now().UnixMicro()
	n, err := b.activeFile.WriteData(key, value, tstamp)
	if err != nil {
		return err
	}

	rec := recfmt.KeyDirRec{
		FileId:    b.activeFile.Name(),
		ValuePos:  uint32(n),
		ValueSize: uint32(len(value)),
		Tstamp:    tstamp,
	}

	b.keyDirMu.Lock()
	b.keyDir[key] = rec
	b.keyDirMu.Unlock()

	return nil
}

func (b *Bitcask) Delete(key string) error {
	if b.usrOpts.accessPermission == ReadOnly {
		return errors.New(fmt.Sprintf("Delete: %s", requireWrite))
	}

	_, err := b.Get(key)
	if err != nil {
		return err
	}

	b.Put(key, datastore.TompStoneValue)

	return nil
}

func (b *Bitcask) ListKeys() []string {
	res := make([]string, 0)

	if b.readerCnt == 0 {
		b.keyDirMu.Lock()
	}
	atomic.AddInt32(&b.readerCnt, 1)

	for key := range b.keyDir {
		res = append(res, key)
	}

	atomic.AddInt32(&b.readerCnt, -1)
	if b.readerCnt == 0 {
		b.keyDirMu.Unlock()
	}

	return res
}

func (b *Bitcask) Fold(fn func(string, string, any) any, acc any) any {
	if b.readerCnt == 0 {
		b.keyDirMu.Lock()
	}
	atomic.AddInt32(&b.readerCnt, 1)

	for key := range b.keyDir {
		value, _ := b.Get(key)
		acc = fn(key, value, acc)
	}

	atomic.AddInt32(&b.readerCnt, -1)
	if b.readerCnt == 0 {
		b.keyDirMu.Unlock()
	}

	return acc
}

func (b *Bitcask) Merge() error {
	if b.usrOpts.accessPermission == ReadOnly {
		return errors.New(fmt.Sprintf("Merge: %s", requireWrite))
	}

	b.keyDirMu.Lock()
	oldKeyDir := b.keyDir
	b.keyDirMu.Unlock()
	newKeyDir := keydir.KeyDir{}
	oldFiles, err := b.listOldFiles()
	if err != nil {
		return err
	}

	mergeFile := datastore.NewAppendFile(b.dataStore.Path(), b.fileFlags, datastore.Merge)
	defer mergeFile.Close()
	for key, rec := range oldKeyDir {
		if rec.FileId != b.activeFile.Name() {
			newRec, err := b.mergeWrite(mergeFile, key)
			if err != nil {
				return err
			}
			newKeyDir[key] = newRec
		} else {
			newKeyDir[key] = rec
		}
	}

	b.keyDirMu.Lock()
	b.keyDir = newKeyDir
	b.keyDirMu.Unlock()

	b.deleteOldFiles(oldFiles)

	return nil
}

func (b *Bitcask) Sync() error {
	if b.usrOpts.accessPermission == ReadOnly {
		return errors.New(fmt.Sprintf("Sync: %s", requireWrite))
	}

	return b.activeFile.Sync()
}

func (b *Bitcask) Close() {
	if b.usrOpts.accessPermission == ReadWrite {
		b.Sync()
		b.activeFile.Close()
	}
	b.dataStore.Close()
}
