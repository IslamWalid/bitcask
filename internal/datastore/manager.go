package datastore

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/IslamWalid/bitcask/internal/recfmt"
	"github.com/IslamWalid/bitcask/internal/sio"
	"github.com/gofrs/flock"
)

const (
	ExclusiveLock LockMode = 0
	SharedLock    LockMode = 1

	lockFile = ".lck"

	accessDenied = "access denied: datastore is locked"
)

// sha256 of "deleted value"
const TompStoneValue = "8890fc70294d02dbde257989e802451c2276be7fb177c3ca4399dc4728e4e1e0"

const KeyNotExist = "key does not exist"

type LockMode int

type DataStore struct {
	path string
	lock LockMode
	flck *flock.Flock
}

func NewDataStore(dataStorePath string, lock LockMode) (*DataStore, error) {
	d := &DataStore{
		path: dataStorePath,
		lock: lock,
	}

	dir, dirErr := os.Open(dataStorePath)
	defer dir.Close()

	if dirErr == nil {
		acquired, err := d.openDataStoreDir()
		if err != nil {
			return nil, err
		}
		if !acquired {
			return nil, errors.New(accessDenied)
		}
	} else if os.IsNotExist(dirErr) && lock == ExclusiveLock {
		err := d.createDataStoreDir()
		if err != nil {
			return nil, err
		}
	} else {
		return nil, dirErr
	}

	return d, nil
}

func NewAppendFile(dataStorePath string, fileFlags int, appendType AppendType) *AppendFile {
	a := &AppendFile{
		filePath:   dataStorePath,
		fileFlags:  fileFlags,
		appendType: appendType,
	}

	return a
}

func (d *DataStore) createDataStoreDir() error {
	err := os.MkdirAll(d.path, os.FileMode(0777))
	if err != nil {
		return err
	}

	_, err = d.acquireFileLock()
	if err != nil {
		return err
	}

	return nil
}

func (d *DataStore) openDataStoreDir() (bool, error) {
	acquired, err := d.acquireFileLock()
	if err != nil {
		return false, err
	}

	if !acquired {
		return acquired, nil
	}

	return true, nil
}

func (d *DataStore) acquireFileLock() (bool, error) {
	var err error
	var ok bool

	d.flck = flock.New(path.Join(d.path, lockFile))
	switch d.lock {
	case ExclusiveLock:
		ok, err = d.flck.TryLock()
	case SharedLock:
		ok, err = d.flck.TryRLock()
	}

	if err != nil {
		return false, err
	}
	return ok, nil
}

func (d *DataStore) ReadValueFromFile(fileId, key string, valuePos, valueSize uint32) (string, error) {
	bufsz := recfmt.DataFileRecHdr + uint32(len(key)) + valueSize
	buf := make([]byte, bufsz)

	f, err := sio.Open(path.Join(d.path, fileId))
	if err != nil {
		return "", err
	}
	defer f.File.Close()

	f.ReadAt(buf, int64(valuePos))
	data, _, err := recfmt.ExtractDataFileRec(buf)
	if err != nil {
		return "", err
	}

	if data.Value == TompStoneValue {
		return "", errors.New(fmt.Sprintf("%s: %s", data.Key, KeyNotExist))
	}

	return data.Value, nil
}

func (d *DataStore) Path() string {
	return d.path
}

func (d *DataStore) Close() {
	d.flck.Unlock()
}
