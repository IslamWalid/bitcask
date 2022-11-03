package bitcask

import (
	"os"
	"path"
	"time"

	"github.com/IslamWalid/bitcask/internal/datastore"
	"github.com/IslamWalid/bitcask/internal/recfmt"
)

// parseUsrOpts fills an options struct with the passed user options.
func parseUsrOpts(opts []ConfigOpt) options {
	usrOpts := options{
		syncOption:       SyncOnDemand,
		accessPermission: ReadOnly,
	}

	for _, opt := range opts {
		switch opt {
		case SyncOnPut:
			usrOpts.syncOption = SyncOnPut
		case ReadWrite:
			usrOpts.accessPermission = ReadWrite
		}
	}

	return usrOpts
}

// listOldFiles prepares a list with all old files to be deleted after merge.
func (b *Bitcask) listOldFiles() ([]string, error) {
	res := make([]string, 0)

	dataStore, err := os.Open(b.dataStore.Path())
	if err != nil {
		return nil, err
	}
	defer dataStore.Close()

	files, err := dataStore.Readdir(0)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		fileName := file.Name()
		if fileName[0] != '.' && fileName != b.activeFile.Name() && fileName != "keydir" {
			res = append(res, fileName)
		}
	}

	return res, nil
}

// mergeWrite performs a writing to the created merge file.
// returns the new record about the written data
// returns error if the data is deleted and will not be written again or on any system failures.
func (b *Bitcask) mergeWrite(mergeFile *datastore.AppendFile, key string) (recfmt.KeyDirRec, error) {
	value, err := b.Get(key)
	if err != nil {
		return recfmt.KeyDirRec{}, err
	}

	tstamp := time.Now().UnixMicro()

	n, err := mergeFile.WriteData(key, value, tstamp)
	if err != nil {
		return recfmt.KeyDirRec{}, err
	}

	newRec := recfmt.KeyDirRec{
		FileId:    mergeFile.Name(),
		ValuePos:  uint32(n),
		ValueSize: uint32(len(value)),
		Tstamp:    tstamp,
	}

	err = mergeFile.WriteHint(key, newRec)
	if err != nil {
		return recfmt.KeyDirRec{}, err
	}

	return newRec, nil
}

// deleteOldFiles deletes all files passed to it.
func (b *Bitcask) deleteOldFiles(files []string) error {
	for _, file := range files {
		err := os.Remove(path.Join(b.dataStore.Path(), file))
		if err != nil {
			return err
		}
	}

	return nil
}
