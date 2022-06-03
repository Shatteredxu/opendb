package logfile

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

const FileName = "opendb.data"
const MergeFileName = "opendb.data.merge"
const mergeDir = "opendb_merge"
var (
	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}
)


// DBFile 数据文件定义
type DBFile struct {
	Id     uint32
	Path   string
	File   *os.File
	Offset int64
}

func newInternal(fileName string,fileId uint32) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	return &DBFile{Id: fileId,Offset: stat.Size(), File: file}, nil
}

// NewDBFile 创建一个新的数据文件
func NewDBFile(path string,fileId uint32,eType uint16) (*DBFile, error) {
	//根据eType和fileId组成不同类型的文件名
	fileName := path + string(os.PathSeparator) + fmt.Sprintf(DBFileFormatNames[eType], fileId)
	return newInternal(fileName,fileId)
}

// NewMergeDBFile 新建一个合并时的数据文件
func NewMergeDBFile(path string,id uint32) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newInternal(fileName,id)
}

// Read 从 offset 处开始读取
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	// read extra info if necessary.
	offset += int64(e.ValueSize)
	if e.ExtraSize > 0 {
		extra := make([]byte, e.ExtraSize)
		if _, err = df.File.ReadAt(extra, offset); err != nil {
			return
		}
		e.Extra = extra
	}
	return
}

// Write 写入 Entry
func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}

// 加载所有归档文件到磁盘中，method FileRWMethod,
func Build(path string,  blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	dir, err := ioutil.ReadDir(path)//从给定的目录中读取文件
	if err != nil {
		return nil, nil, err
	}

	// build merged files if necessary.
	// merge path is a sub directory in path.
	var (
		mergedFiles map[uint16]map[uint32]*DBFile
		mErr        error
	)
	for _, d := range dir {
		if d.IsDir() && strings.Contains(d.Name(), mergeDir) {
			mergePath := path + string(os.PathSeparator) + d.Name()
			if mergedFiles, _, mErr = Build(mergePath, blockSize); mErr != nil {
				return nil, nil, mErr
			}
		}
	}

	fileIdsMap := make(map[uint16][]int)
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") {
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])

			// find the different types of file.
			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	// load all the db files.
	activeFileIds := make(map[uint16]uint32)
	archFiles := make(map[uint16]map[uint32]*DBFile)
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIDs := fileIdsMap[dataType]
		sort.Ints(fileIDs)
		files := make(map[uint32]*DBFile)
		var activeFileId uint32 = 0
		if len(fileIDs) > 0 {
			activeFileId = uint32(fileIDs[len(fileIDs)-1])
			length := len(fileIDs) - 1
			if strings.Contains(path, mergeDir) {
				length++
			}
			for i := 0; i < length; i++ {
				id := fileIDs[i]

				file, err := NewDBFile(path, uint32(id),  dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}
		archFiles[dataType] = files
		activeFileIds[dataType] = activeFileId
	}

	// merged files are also archived files.
	if mergedFiles != nil {
		for dType, file := range archFiles {
			if mergedFile, ok := mergedFiles[dType]; ok {
				for id, f := range mergedFile {
					file[id] = f
				}
			}
		}
	}
	return archFiles, activeFileIds, nil
}