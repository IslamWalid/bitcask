package recfmt

import (
	"encoding/binary"
	"strconv"
)

const keyDirFileHdr = 26

type KeyDirRec struct {
	FileId    string
	ValuePos  uint32
	ValueSize uint32
	Tstamp    int64
}

func CompressKeyDirRec(key string, rec KeyDirRec) []byte {
	keySize := len(key)
	buf := make([]byte, keyDirFileHdr+keySize)
	fid, _ := strconv.ParseUint(rec.FileId, 10, 64)
	binary.LittleEndian.PutUint64(buf, fid)
	binary.LittleEndian.PutUint16(buf[8:], uint16(keySize))
	binary.LittleEndian.PutUint32(buf[10:], rec.ValueSize)
	binary.LittleEndian.PutUint32(buf[14:], rec.ValuePos)
	binary.LittleEndian.PutUint64(buf[18:], uint64(rec.Tstamp))
	copy(buf[26:], []byte(key))

	return buf
}

func ExtractKeyDirRec(rec []byte) (string, KeyDirRec, int) {
	fileId := strconv.FormatUint(binary.LittleEndian.Uint64(rec), 10)
	keySize := binary.LittleEndian.Uint16(rec[8:])
	valueSize := binary.LittleEndian.Uint32(rec[10:])
	valuePos := binary.LittleEndian.Uint32(rec[14:])
	tstamp := binary.LittleEndian.Uint64(rec[18:])
	key := string(rec[26 : keySize+26])

	return key, KeyDirRec{
		FileId:    fileId,
		ValuePos:  valuePos,
		ValueSize: valueSize,
		Tstamp:    int64(tstamp),
	}, keyDirFileHdr + int(keySize)
}
