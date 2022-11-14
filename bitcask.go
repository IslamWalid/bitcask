// Package bitcask provides functionality to create and manipulate a key-value datastore.
package bitcask

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IslamWalid/bitcask/internal/datastore"
	"github.com/IslamWalid/bitcask/internal/keydir"
	"github.com/IslamWalid/bitcask/internal/recfmt"
)

const (
	// ReadOnly gives the bitcask process a read only permission.
	ReadOnly ConfigOpt = 0
	// ReadWrite gives the bitcask process read and write permissions.
	ReadWrite ConfigOpt = 1
	// SyncOnPut makes the bitcask flush all the writes directly to the disk.
	SyncOnPut ConfigOpt = 2
	// SyncOnDemand gives the user the control on whenever to do flush operation.
	SyncOnDemand ConfigOpt = 3
)

// errRequireWrite happens whenever a user with ReadOnly permission tries to do a writing operation.
var errRequireWrite = errors.New("require write permission")

type (
	// ConfigOpt represents the config options the user can have.
	ConfigOpt int

	// options groups the config options passed to Open.
	options struct {
		syncOption       ConfigOpt
		accessPermission ConfigOpt
	}

	// Bitcask represents the bitcask object.
	// Bitcask contains the metadata needed to manipulate the bitcask datastore.
	// User creates an object of it with to use the bitcask.
	// Provides several methods to manipulate the datastore data.
	Bitcask struct {
		keyDir     keydir.KeyDir
		usrOpts    options
		keyDirMu   sync.Mutex
		readerCnt  int32
		dataStore  *datastore.DataStore
		activeFile *datastore.AppendFile
		fileFlags  int
	}
)

// Open creates a new bitcask object to manipulate the given datastore path.
// It can take options ReadWrite, ReadOnly, SyncOnPut and SyncOnDemand as config options.
// Only one ReadWrite process can open a bitcask at a time.
// Only ReadWrite permission can create a new bitcask datastore.
// Multiple Readers or a single writer is allowed to be in the same datastore in the same time.
// If there is no bitcask datastore in the given path a new datastore is created when ReadWrite permission is given.
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

// Get retrieves the value by key from a bitcask datastore.
// Return an error if key does not exist in the bitcask datastore.
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
		return "", errors.New(fmt.Sprintf("%s: %s", key, datastore.ErrKeyNotExist))
	}

	return b.dataStore.ReadValueFromFile(rec.FileId, key, rec.ValuePos, rec.ValueSize)
}

// Put stores a value by key in a bitcask datastore.
// Return an error on any system failure when writing the data.
func (b *Bitcask) Put(key, value string) error {
	if b.usrOpts.accessPermission == ReadOnly {
		return errors.New(fmt.Sprintf("Put: %s", errRequireWrite))
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

// Delete removes a key from a bitcask datastore
// by appending a special TompStone value that will be deleted in the next merge.
// Return an error if key does not exist in the bitcask datastore.
func (b *Bitcask) Delete(key string) error {
	if b.usrOpts.accessPermission == ReadOnly {
		return errors.New(fmt.Sprintf("Delete: %s", errRequireWrite))
	}

	_, err := b.Get(key)
	if err != nil {
		return err
	}

	b.Put(key, datastore.TompStone)

	return nil
}

// ListKeys list all keys in a bitcask datastore.
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

// Fold folds over all key/value pairs in a bitcask datastore.
// fun is expected to be in the form: F(K, V, Acc) -> Acc
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

// Merge rearrange the bitcask datastore in a more compact form.
// Delete values with older timestamps.
// Reduces the disk usage after as it deletes unneeded values.
// Produces hintfiles to provide a faster startup.
// Return an error if ReadWrite permission is not set or on any system failures when writing data.
func (b *Bitcask) Merge() error {
	if b.usrOpts.accessPermission == ReadOnly {
		return errors.New(fmt.Sprintf("Merge: %s", errRequireWrite))
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
				if !strings.HasSuffix(err.Error(), datastore.ErrKeyNotExist.Error()) {
					return err
				}
			} else {
				newKeyDir[key] = newRec
			}
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

// Sync flushes all data to the disk.
// Return an error if ReadWrite permission is not set.
func (b *Bitcask) Sync() error {
	if b.usrOpts.accessPermission == ReadOnly {
		return errors.New(fmt.Sprintf("Sync: %s", errRequireWrite))
	}

	return b.activeFile.Sync()
}

// Close flushes all data to the disk and closes the bitcask datastore.
// After close the bitcask object cannot be used anymore.
func (b *Bitcask) Close() {
	if b.usrOpts.accessPermission == ReadWrite {
		b.Sync()
		b.activeFile.Close()
	}
	b.dataStore.Close()
}
