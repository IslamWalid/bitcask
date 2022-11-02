package recfmt

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const DataFileRecHdr = 18
const dataCorruption = "corrution detected: datastore files are corrupted"

type DataRec struct {
	Key       string
	Value     string
	Tstamp    int64
	KeySize   uint16
	ValueSize uint32
}

func CompressDataFileRec(key, value string, tstamp int64) []byte {
	rec := make([]byte, DataFileRecHdr+len(key)+len(value))

	binary.LittleEndian.PutUint64(rec[4:], uint64(tstamp))
	binary.LittleEndian.PutUint16(rec[12:], uint16(len(key)))
	binary.LittleEndian.PutUint32(rec[14:], uint32(len(value)))
	copy(rec[DataFileRecHdr:], []byte(key))
	copy(rec[DataFileRecHdr+len(key):], []byte(value))

	checkSum := crc32.ChecksumIEEE(rec[4:])
	binary.LittleEndian.PutUint32(rec, checkSum)

	return rec
}

func ExtractDataFileRec(rec []byte) (*DataRec, uint32, error) {
	parsedSum := binary.LittleEndian.Uint32(rec)
	tstamp := binary.LittleEndian.Uint64(rec[4:])
	keySize := binary.LittleEndian.Uint16(rec[12:])
	valueSize := binary.LittleEndian.Uint32(rec[14:])
	key := string(rec[DataFileRecHdr : DataFileRecHdr+keySize])
	valueOffset := uint32(DataFileRecHdr + keySize)
	value := string(rec[valueOffset : valueOffset+valueSize])

	err := validateCheckSum(parsedSum, rec[4:DataFileRecHdr+uint32(keySize)+valueSize])
	if err != nil {
		return nil, 0, err
	}

	return &DataRec{
		Key:       key,
		Value:     value,
		Tstamp:    int64(tstamp),
		KeySize:   keySize,
		ValueSize: valueSize,
	}, DataFileRecHdr + valueSize + uint32(keySize), nil
}

func validateCheckSum(parsedSum uint32, rec []byte) error {
	wantedSum := crc32.ChecksumIEEE(rec)
	if parsedSum != wantedSum {
		return errors.New(dataCorruption)
	}

	return nil
}
