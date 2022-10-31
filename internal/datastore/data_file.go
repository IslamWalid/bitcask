package datastore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"time"

	"github.com/IslamWalid/bitcask/internal/sio"
)

const maxFileSize = 10 * 1024
const dataCorruption = "corrution detected: datastore files are corrupted"
const recFileHeader = 18

type DataFile struct {
    file        *sio.File
    fileName    string
    filePath    string
    fileFlags   int
    currentPos  int
    currentSize int
}

type DataRecord struct {
    key         string
    value       string
    tstamp      uint64
    keySize     uint16
    valueSize   uint32
}

func (f *DataFile) Write(key, value string, tstamp uint64) (int, error) {
    rec := f.compress(key, value, tstamp)

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

func (f *DataFile) compress(key, value string, tstamp uint64) []byte {
    rec := make([]byte, recFileHeader + len(key) + len(value))

    binary.LittleEndian.PutUint64(rec[4:], tstamp)
    binary.LittleEndian.PutUint16(rec[12:], uint16(len(key)))
    binary.LittleEndian.PutUint32(rec[14:], uint32(len(value)))
    copy(rec[recFileHeader:], []byte(key))
    copy(rec[recFileHeader + len(key):], []byte(value))

    checkSum := crc32.ChecksumIEEE(rec[4:])
    binary.LittleEndian.PutUint32(rec, checkSum)

    return rec
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

func (f *DataFile) Read(pos, keySize, valueSize int) (*DataRecord, error) {
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

    return f.extract(rec)
}

func (f *DataFile) extract(rec []byte) (*DataRecord, error) {
    parsedSum := binary.LittleEndian.Uint32(rec)
    err := validateCheckSum(parsedSum, rec[4:])
    if err != nil {
        return nil, err
    }

    tstamp := binary.LittleEndian.Uint64(rec[4:])
    keySize := binary.LittleEndian.Uint16(rec[12:])
    valueSize := binary.LittleEndian.Uint32(rec[14:])
    key := string(rec[recFileHeader : recFileHeader+keySize])
    valueOffset := uint32(recFileHeader + keySize)
    value := string(rec[valueOffset : valueOffset+valueSize])

    return &DataRecord{
    	key:       key,
    	value:     value,
    	tstamp:    tstamp,
    	keySize:   keySize,
    	valueSize: valueSize,
    }, nil
}

func validateCheckSum(parsedSum uint32, rec []byte) error {
    wantedSum := crc32.ChecksumIEEE(rec)
    if parsedSum != wantedSum {
        return DataStoreError(dataCorruption)
    }

    return nil
}
