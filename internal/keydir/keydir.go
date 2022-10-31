package keydir

import (
	"os"
	"path"
	"strings"

	"github.com/IslamWalid/bitcask/internal/recfmt"
	"github.com/IslamWalid/bitcask/internal/sio"
)

const keyDirFile = "keydir"

const (
    PrivateKeyDir   KeyDirPrivacy = 0
    SharedKeyDir    KeyDirPrivacy = 1
)

const (
    data fileType = 0
    hint fileType = 1
)

type fileType int

type KeyDirPrivacy int

type KeyDir map[string]recfmt.KeyDirRec

func New(dataStorePath string, privacy KeyDirPrivacy) (KeyDir, error) {
    k := KeyDir{}

    okay, err := k.keyDirFileBuild(dataStorePath)
    if err != nil {
        return nil, err
    }
    if okay {
        return k, nil
    }

    err = k.dataFilesBuild(dataStorePath)
    if err != nil {
        return nil, err
    }

    if privacy == SharedKeyDir {
        k.share(dataStorePath)
    }
    
    return k, nil
}

func (k KeyDir) keyDirFileBuild(dataStorePath string) (bool, error) {
    data, err := os.ReadFile(path.Join(dataStorePath, keyDirFile))
    if err != nil {
        if os.IsNotExist(err) {
            return false, nil
        }
        return false, err
    }

    okay, err := isOld(dataStorePath)
    if err != nil {
        return false, nil
    }
    if !okay {
        return okay, nil
    }

    i := 0
    n := len(data)
    for i < n {
        key, rec, recLen := recfmt.ExtractKeyDirRec(data[i:])
        k[key] = rec
        i += recLen
    }

    return true, nil
}

func isOld(dataStorePath string) (bool, error) {
    dataStoreStat, err := os.Stat(dataStorePath)
    if err != nil {
        return false, err
    }
    
    keydirStat, err := os.Stat(path.Join(dataStorePath, "keydir"))
    if err != nil {
        return false, err
    }
    
    return keydirStat.ModTime().Before(dataStoreStat.ModTime()), nil
}

func (k KeyDir) dataFilesBuild(dataStorePath string) error {
    dataStore, err := os.Open(dataStorePath)
    if err != nil {
        return err
    }
    defer dataStore.Close()
    files, err := dataStore.Readdir(0)
    if err != nil {
        return err
    }

    fileNames := make([]string, 0)
    for _, file := range files {
        fileNames = append(fileNames, file.Name())
    }

    k.parseFiles(dataStorePath, categorizeFiles(fileNames))
    if err != nil {
        return err
    }

    return nil
}

func (k KeyDir) parseFiles(dataStorePath string, files map[string]fileType) error {
    for name, ftype := range files {
        switch ftype {
        case data:
            err := k.parseDataFile(dataStorePath, name)
            if err != nil {
                return err
            }
        case hint:
            err := k.parseHintFile(dataStorePath, name)
            if err != nil {
                return err
            }
        }
    }

    return nil
}

func (k KeyDir) parseDataFile(dataStorePath, name string) error {
    data, err := os.ReadFile(path.Join(dataStorePath, name))
    if err != nil {
        return err
    }
    
    i := 0
    n := len(data)
    for i < n {
        dataRec, recLen, err := recfmt.ExtractDataFileRec(data[i:])
        if err != nil {
            return err
        }

        k[dataRec.Key] = recfmt.KeyDirRec{
        	FileId:    name,
        	ValuePos:  uint32(i),
        	ValueSize: uint32(len(dataRec.Value)),
        	Tstamp:    dataRec.Tstamp,
        }
        i += int(recLen)
    }

    return nil
}

func (k KeyDir) parseHintFile(dataStorePath, name string) error {
    return nil
}

func categorizeFiles(allFiles []string) map[string]fileType {
    res := make(map[string]fileType)

    hintFiles := make(map[string]int)
    for _, file := range allFiles {
        if strings.HasSuffix(file, ".hint") {
            fileWithoutExt := strings.Trim(file, ".hint")
            hintFiles[fileWithoutExt] = 1
            res[file] = hint
        }
    }

    for _, file := range allFiles {
        if strings.HasSuffix(file, ".data") {
            if _, okay := hintFiles[strings.Trim(file, ".data")]; !okay {
                res[file] = data
            }
        }
    }

    return res
}

func (k KeyDir) share(dataStorePath string) error {
    flags := os.O_CREATE | os.O_RDWR
    perm := os.FileMode(666)
    file, err := sio.OpenFile(path.Join(dataStorePath, "keydir"), flags, perm)
    if err != nil {
        return err
    }

    for key, rec := range k {
        buf := recfmt.CompressKeyDirRec(key, rec)
        _, err := file.Write(buf)
        if err != nil {
            return err
        }
    }

    return nil
}
