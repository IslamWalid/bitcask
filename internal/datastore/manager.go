package datastore

import (
	"os"
	"path"

	"github.com/gofrs/flock"
)

const (
    ExclusiveLock   LockMode = 0
    SharedLock      LockMode = 1

    lockFile = ".lck"

    accessDenied = "access denied: datastore is locked"
)

type LockMode int

type DataStoreError string

func (e DataStoreError) Error() string {
    return string(e)
}

type DataStore struct {
    path string
    lock LockMode
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
            return nil, DataStoreError(accessDenied)
        }
    } else if os.IsNotExist(dirErr) {
        err := d.createDataStoreDir()
        if err != nil {
            return nil, err
        }
    } else {
        return nil, dirErr
    }

    return d, nil
}

func (d *DataStore) createDataStoreDir() error {
    err := os.MkdirAll(d.path, os.FileMode(0777))
    if err != nil {
        return err
    }

    lfile, err := os.Create(path.Join(d.path, lockFile))
    if err != nil {
        return err
    }
    defer lfile.Close()

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

    flck := flock.New(d.path)
    switch d.lock {
    case ExclusiveLock:
        ok, err = flck.TryLock()
    case SharedLock:
        ok, err = flck.TryRLock()
    }

    if err != nil {
        return false, err
    }
    return ok, nil
}
