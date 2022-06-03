package opendb

import "time"

// DataIndexMode the data index mode.
type DataIndexMode int

const (
	KeyValueMemMode DataIndexMode = iota

	KeyOnlyMemMode
)

type IOType int8

const (
	// FileIO standard file io.iota代表重置为0
	FileIO IOType = iota
	MMap// 因为iota，这里初始化为1
)

// Options for opening a db.
type Options struct {
	DBPath string
	IdxMode DataIndexMode
	IoType IOType
	Sync bool
	LogFileGCInterval time.Duration
	LogFileGCRatio float64
	DefaultBlockSize int64
}

// 默认设置，如果用户没有自己定义则使用
func DefaultOptions(path string) Options {
	return Options{
		DBPath:               path,
		IdxMode:            KeyOnlyMemMode,
		IoType:               MMap,
		Sync:                 false,
		LogFileGCInterval:    time.Hour * 8,
		LogFileGCRatio:       0.5,
		DefaultBlockSize: 32 << 20,//为32
		//DiscardBufferSize:    4 << 12,
	}
}
