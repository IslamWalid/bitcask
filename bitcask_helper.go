package bitcask

import (
	"errors"
	"fmt"

	"github.com/IslamWalid/bitcask/internal/recfmt"
	"github.com/IslamWalid/bitcask/internal/sio"
)

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

func getValueFromFile(rec recfmt.KeyDirRec, keySize int) (string, error) {
	bufsz := recfmt.DataFileRecHdr + uint32(keySize) + rec.ValueSize
	buf := make([]byte, bufsz)
	f, err := sio.Open(rec.FileId)
	if err != nil {
		return "", err
	}
	defer f.File.Close()

	f.ReadAt(buf, int64(rec.ValuePos))
	data, _, err := recfmt.ExtractDataFileRec(buf)
	if err != nil {
		return "", err
	}

	if data.Value == tompStoneValue {
		return "", errors.New(fmt.Sprintf("%s: %s", data.Key, keyNotExist))
	}

	return data.Value, nil
}