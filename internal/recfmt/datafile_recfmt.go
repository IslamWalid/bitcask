package recfmt

import (
	"encoding/binary"
	"hash/crc32"
)

const dataFileRecHdr = 18
const dataCorruption = "corrution detected: datastore files are corrupted"

type RecFmtError string

func (e RecFmtError) Error() string {
    return string(e)
}

type DataRec struct {
    Key         string
    Value       string
    Tstamp      uint64
    KeySize     uint16
    ValueSize   uint32
}

func CompressDataFileRec(key, value string, tstamp uint64) []byte {
    rec := make([]byte, dataFileRecHdr + len(key) + len(value))

    binary.LittleEndian.PutUint64(rec[4:], tstamp)
    binary.LittleEndian.PutUint16(rec[12:], uint16(len(key)))
    binary.LittleEndian.PutUint32(rec[14:], uint32(len(value)))
    copy(rec[dataFileRecHdr:], []byte(key))
    copy(rec[dataFileRecHdr + len(key):], []byte(value))

    checkSum := crc32.ChecksumIEEE(rec[4:])
    binary.LittleEndian.PutUint32(rec, checkSum)

    return rec
}

func ExtractDataFileRec(rec []byte) (*DataRec, uint32, error) {
    parsedSum := binary.LittleEndian.Uint32(rec)
    err := validateCheckSum(parsedSum, rec[4:])
    if err != nil {
        return nil, 0, err
    }

    tstamp := binary.LittleEndian.Uint64(rec[4:])
    keySize := binary.LittleEndian.Uint16(rec[12:])
    valueSize := binary.LittleEndian.Uint32(rec[14:])
    key := string(rec[dataFileRecHdr : dataFileRecHdr+keySize])
    valueOffset := uint32(dataFileRecHdr + keySize)
    value := string(rec[valueOffset : valueOffset+valueSize])

    return &DataRec{
    	Key:       key,
    	Value:     value,
    	Tstamp:    tstamp,
    	KeySize:   keySize,
    	ValueSize: valueSize,
    }, dataFileRecHdr + valueSize + uint32(keySize), nil
}

func validateCheckSum(parsedSum uint32, rec []byte) error {
    wantedSum := crc32.ChecksumIEEE(rec)
    if parsedSum != wantedSum {
        return RecFmtError(dataCorruption)
    }

    return nil
}
