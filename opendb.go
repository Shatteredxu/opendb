package opendb

import (
	"errors"
	"io"
	"log"
	"opendb/util"
	"sort"
	//"io/ioutil"
	"opendb/logfile"
	"os"
	"sync"
	//"opendb/log_entry"

)
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)
const (
	StringSet uint16 = iota
	StringRem
	StringExpire
	StringPersist
)
var (
	// ErrEmptyKey the key is empty
	ErrEmptyKey = errors.New("opendb: the key is empty")

	// ErrKeyNotExist key not exist
	ErrKeyNotExist = errors.New("opendb: key not exist")

	// ErrKeyTooLarge the key too large
	ErrKeyTooLarge = errors.New("opendb: key exceeded the max length")

	// ErrValueTooLarge the value too large
	ErrValueTooLarge = errors.New("opendb: value exceeded the max length")

	// ErrNilIndexer the indexer is nil
	ErrNilIndexer = errors.New("opendb: indexer is nil")

	// ErrMergeUnreached not ready to reclaim
	ErrMergeUnreached = errors.New("opendb: unused space not reach the threshold")

	// ErrExtraContainsSeparator extra contains separator
	ErrExtraContainsSeparator = errors.New("opendb: extra contains separator \\0")

	// ErrInvalidTTL ttl is invalid
	ErrInvalidTTL = errors.New("opendb: invalid ttl")

	// ErrKeyExpired the key is expired
	ErrKeyExpired = errors.New("opendb: key is expired")

	// ErrDBisMerging merge and single merge can`t execute at the same time.
	ErrDBisMerging = errors.New("opendb: can`t do reclaim and single reclaim at the same time")

	// ErrDBIsClosed db can`t be used after closed.
	ErrDBIsClosed = errors.New("opendb: db is closed, reopen it")

	// ErrTxIsFinished tx is finished.
	ErrTxIsFinished = errors.New("opendb: transaction is finished, create a new one")

	// ErrActiveFileIsNil active file is nil.
	ErrActiveFileIsNil = errors.New("opendb: active file is nil")

	// ErrWrongNumberOfArgs wrong number of arguments
	ErrWrongNumberOfArgs = errors.New("opendb: wrong number of arguments")
)
var DataStructureNum = 5
type (
	DataType = uint16
	OpenDB struct {
		indexes map[string]int64 // 内存中的索引信息
		dbFile  *logfile.DBFile          // 数据文件
		dirPath string           // 数据目录
		opts   Options         //配置文件
		activeFile      *sync.Map //目前写入的文件
		archFiles       ArchivedFiles // map+map+dbfile
		expires         Expires       // 设定了过期的键列表集合
		mu      sync.RWMutex
		strIndex        *StrIdx       // String indexes(a skip list).
		listIndex       *ListIdx      // List indexes.
		hashIndex       *HashIdx      // Hash indexes.
		setIndex        *SetIdx       // Set indexes.
		zsetIndex       *ZsetIdx      // Sorted set indexes.

	}
	//map+map+dbfile 第一个map代表不同数据类型的归档文件，第二个代表归档文件的集合
	ArchivedFiles map[DataType]map[uint32]*logfile.DBFile
	Expires map[DataType]map[string]int64
)

// Open 开启一个数据库实例
func Open(opts Options) (*OpenDB, error) {
	// 1.如果路径不存在，则创建一个
	if !util.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//2.获取文件锁，防止多线程操作同一个文件 TODO

	// 3.加载数据文件,构建数据库实例
	archFiles, activeFileIds, err := logfile.Build(opts.DBPath, opts.DefaultBlockSize)
	if err != nil {
		return nil, err
	}
	activeFiles := new(sync.Map)
	for dataType, fileId := range activeFileIds {
		file, err := logfile.NewDBFile(opts.DBPath, fileId,dataType)
		if err != nil {
			return nil, err
		}
		activeFiles.Store(dataType, file)
	}

	db := &OpenDB{
		//dbFile:  dbFile,
		archFiles: archFiles,
		activeFile: activeFiles,
		indexes: make(map[string]int64),
		dirPath: opts.DBPath,
		opts: opts,
		//strIndex:   newStrIdx(),
		listIndex:  newListIdx(),
		hashIndex:  newHashIdx(),
		setIndex:   newSetIdx(),
		zsetIndex:  newZsetIdx(),
	}

	// 扫描文件，加载索引到内存。
	db.loadIdxFromFiles()
	return db, nil
}



//// Put 写入数据
//func (db *OpenDB) Put(key []byte, value []byte) (err error) {
//	if len(key) == 0 {
//		return
//	}
//
//	db.mu.Lock()
//	defer db.mu.Unlock()
//
//
//	// 封装成 Entry
//	entry := logfile.NewEntryNoExtra(key, value, String,StringSet)
//	// 追加到数据文件当中
//	err = db.store(entry)//0暂时代表写入str文件
//	// 写到内存
//	activeFile, err :=db.getActiveFile(0)
//	db.indexes[string(key)] = activeFile.Offset-entry.GetSize()
//	return
//}
//
//// Get 取出数据
//func (db *OpenDB) Get(key []byte) (val []byte, err error) {
//	if len(key) == 0 {
//		return
//	}
//
//	db.mu.RLock()
//	defer db.mu.RUnlock()
//
//	// 从内存当中取出索引信息
//	offset, ok := db.indexes[string(key)]
//	// key 不存在
//	if !ok {
//		return
//	}
//
//	// 从磁盘中读取数据
//	var e *logfile.Entry
//	activeFile, err :=db.getActiveFile(0)
//	e, err = activeFile.Read(offset)
//	if err != nil && err != io.EOF {
//		return
//	}
//	if e != nil {
//		val = e.Value
//	}
//	return
//}
//
//// Del 删除数据
//func (db *OpenDB) Del(key []byte) (err error) {
//	if len(key) == 0 {
//		return
//	}
//
//	db.mu.Lock()
//	defer db.mu.Unlock()
//	// 从内存当中取出索引信息
//	_, ok := db.indexes[string(key)]
//	// key 不存在，忽略
//	if !ok {
//		return
//	}
//
//	// 封装成 Entry 并写入
//	e := logfile.NewEntry(key, nil,nil ,String,StringRem)
//	err = db.dbFile.Write(e)
//	if err != nil {
//		return
//	}
//
//	// 删除内存中的 key
//	delete(db.indexes, string(key))
//	return
//}
//将entry写入活跃文件中
func (db *OpenDB) store(e *logfile.Entry) error {
	// sync the db file if file size is not enough, and open a new db file.
	id := e.Mark
	config := db.opts
	activeFile, err := db.getActiveFile(id)//获得给定类型的固定活跃文件
	if err != nil {
		return err
	}

	if activeFile.Offset+int64(e.GetSize()) > config.DefaultBlockSize {
		//if err := activeFile.Sync(); err != nil {//将文件通过sync持久化到磁盘
		//	return err
		//}

		// save the old db file as arched file.
		activeFileId := activeFile.Id
		db.archFiles[e.Mark][activeFileId] = activeFile

		newDbFile, err := logfile.NewDBFile(db.opts.DBPath, activeFileId+1, e.Mark)
		if err != nil {
			return err
		}
		activeFile = newDbFile
	}

	// write entry to db file.
	if err := activeFile.Write(e); err != nil {
		return err
	}
	db.activeFile.Store(id, activeFile)

	// persist db file according to the config.
	//if config.Sync {
	//	if err := activeFile.Sync(); err != nil {
	//		return err
	//	}
	//}
	return nil
}
//根据给定类型，返回活跃文件
func (db *OpenDB) getActiveFile(dType DataType) (file *logfile.DBFile, err error) {
	value, ok := db.activeFile.Load(dType)//从map中查找
	if !ok || value == nil {
		return nil, ErrActiveFileIsNil
	}
	var typeOk bool
	if file, typeOk = value.(*logfile.DBFile); !typeOk {
		return nil, ErrActiveFileIsNil
	}
	return
}

// load String、List、Hash、Set、ZSet indexes from db files.
func (db *OpenDB) loadIdxFromFiles() error {
 	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	//wg := sync.WaitGroup{}
	//wg.Add(DataStructureNum)
	for dataType := 0; dataType < DataStructureNum; dataType++ {
		//go func(dType uint16) {
		//	defer wg.Done()
			// archived files
			var fileIds []int
			dbFile := make(map[uint32]*logfile.DBFile)
			v:=db.archFiles[uint16(dataType)]
			println(v)
			for k, v := range db.archFiles[uint16(dataType)] {
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}

			// active file返回id，代表这个类型活跃文件的id
			activeFile, err := db.getActiveFile(uint16(dataType))
			if err != nil {
				log.Fatalf("active file is nil, the db can not open.[%+v]", err)
				return err
			}
			dbFile[activeFile.Id] = activeFile
			fileIds = append(fileIds, int(activeFile.Id))

			// load the db files in a specified order.
			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= db.opts.DefaultBlockSize {
					if e, err := df.Read(offset); err == nil {
						// 设置索引状态，最初的版本，只有Stringindex，也就是字符串类型时候，
						idx := &Index{
							FileId: fid,
							Offset: offset,
						}
						idx.Meta.Key = e.Key
						idx.Meta.Value = e.Value
						idx.Meta.Extra = e.Extra
						offset += int64(e.GetSize())
						e.Mark = uint16(dataType);
						//当有多个索引的时候，在加载时就需要对应不同类型的索引进行加载
						if len(e.Key) > 0 {
							if err := db.buildIndex(e, idx, true); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}
						}
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
					}
				}
			}
		//}(uint16(dataType))
	}
	//wg.Wait()
	return nil
}
// Merge 合并数据文件，在rosedb当中是 Reclaim 方法
func (db *OpenDB) Merge() error {
	// 没有数据，忽略
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*logfile.Entry
		offset       int64
	)

	// 读取原数据文件中的 Entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) > 0 {
		// 新建临时文件
		mergeDBFile, err := logfile.NewMergeDBFile(db.dirPath,1)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBFile.File.Name())

		// 重新写入有效的 entry
		for _, entry := range validEntries {
			writeOff := mergeDBFile.Offset
			err := mergeDBFile.Write(entry)
			if err != nil {
				return err
			}

			// 更新索引
			db.indexes[string(entry.Key)] = writeOff
		}

		// 获取文件名
		dbFileName := db.dbFile.File.Name()
		// 关闭文件
		db.dbFile.File.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)

		// 获取文件名
		mergeDBFileName := mergeDBFile.File.Name()
		// 关闭文件
		mergeDBFile.File.Close()
		// 临时文件变更为新的数据文件
		os.Rename(mergeDBFileName, db.dirPath+string(os.PathSeparator)+logfile.FileName)

		db.dbFile = mergeDBFile
	}
	return nil
}


func (db *OpenDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}
	//TODO 可以对keyvalue进行一定的限制
	//config := db.opts
	//if keySize > config.MaxKeySize {
	//	return ErrKeyTooLarge
	//}
	//
	//for _, v := range value {
	//	if uint32(len(v)) > config.MaxValueSize {
	//		return ErrValueTooLarge
	//	}
	//}

	return nil
}
// TODO 对键值对进行过期检查
func (db *OpenDB) checkExpired(key []byte, dType DataType) (expired bool) {

	return
}
// build the indexes for different data structures.
func (db *OpenDB) buildIndex(entry *logfile.Entry, idx *Index, isOpen bool) (err error) {
	//设置Index的存储模式
	//if db.opts.IdxMode == KeyValueMemMode && entry.GetType() == String {
	//	idx.Meta.Value = entry.Meta.Value
	//	idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	//}


	switch entry.GetMark() {
	case String:
		db.buildStringIndex(idx, entry)
	case List:
		db.buildListIndex(entry)
	case Hash:
		db.buildHashIndex(entry)
	case Set:
		db.buildSetIndex(entry)
	case ZSet:
		db.buildZsetIndex(entry)
	}
	return
}
func (db *OpenDB) encode(key, value interface{}) (encKey, encVal []byte, err error) {
	if encKey, err = util.EncodeKey(key); err != nil {
		return
	}
	if encVal, err = util.EncodeValue(value); err != nil {
		return
	}
	return
}