package opendb
/**
	对于不同的存储类型，设置其对应的索引，5种
 */
import (
	"opendb/ds/list"
	"opendb/logfile"
	"opendb/util"
	"strconv"
	"strings"
)
//type DataType = uint16
type Index struct {
	Meta  struct{
		Key       []byte
		Value     []byte
		Extra     []byte
	}
	FileId uint32        // the file id of storing the data.
	Offset int64         // entry data query start position.
}
// build string indexes.
func (db *OpenDB) buildStringIndex(idx *Index, entry *logfile.Entry) {
	if db.strIndex == nil || idx == nil {
		return
	}

	switch entry.GetMark() {
	case StringSet:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
	case StringRem:
		db.strIndex.idxList.Remove(idx.Meta.Key)
	case StringExpire:
		//if entry.Timestamp < uint64(time.Now().Unix()) {
		//	db.strIndex.idxList.Remove(idx.Meta.Key)
		//} else {
		//	db.expires[String][string(idx.Meta.Key)] = int64(entry.Timestamp)
		//	db.strIndex.idxList.Put(idx.Meta.Key, idx)
		//}
	case StringPersist:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
		delete(db.expires[String], string(idx.Meta.Key))
	}
}

// build list indexes.
func (db *OpenDB) buildListIndex(entry *logfile.Entry) {
	if db.listIndex == nil || entry == nil {
		return
	}

	key := string(entry.Key)
	switch entry.GetMark() {
	case ListLPush:
		db.listIndex.indexes.LPush(key, entry.Value)
	case ListLPop:
		db.listIndex.indexes.LPop(key)
	case ListRPush:
		db.listIndex.indexes.RPush(key, entry.Value)
	case ListRPop:
		db.listIndex.indexes.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(entry .Extra)); err == nil {
			db.listIndex.indexes.LRem(key, entry.Value, count)
		}
	case ListLInsert:
		extra := string(entry.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.indexes.LInsert(string(entry.Key), list.InsertOption(opt), pivot, entry.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(entry.Extra)); err == nil {
			db.listIndex.indexes.LSet(key, i, entry.Value)
		}
	case ListLTrim:
		extra := string(entry.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.indexes.LTrim(string(entry.Key), start, end)
		}
	//case ListLExpire:
	//	if entry.Timestamp < uint64(time.Now().Unix()) {
	//		db.listIndex.indexes.LClear(key)
	//	} else {
	//		db.expires[List][key] = int64(entry.Timestamp)
	//	}
	case ListLClear:
		db.listIndex.indexes.LClear(key)
	}
}

// build hash indexes.
func (db *OpenDB) buildHashIndex(entry *logfile.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Key)
	switch entry.GetMark() {
	case HashHSet:
		db.hashIndex.indexes.HSet(key, string(entry.Extra), entry.Value)
	case HashHDel:
		db.hashIndex.indexes.HDel(key, string(entry.Extra))
	case HashHClear:
		db.hashIndex.indexes.HClear(key)
		//case HashHExpire:
		//	if entry.Timestamp < uint64(time.Now().Unix()) {
		//		db.hashIndex.indexes.HClear(key)
		//	} else {
		//		db.expires[Hash][key] = int64(entry.Timestamp)
		//	}
		//}
	}
}
// build set indexes.
func (db *OpenDB) buildSetIndex(entry *logfile.Entry){
	if db.hashIndex == nil || entry == nil {
		return
	}
	key := string(entry.Key)
	switch entry.GetMark() {
	case SetSAdd:
		db.setIndex.indexes.SAdd(key, entry.Value)
	case SetSRem:
		db.setIndex.indexes.SRem(key, entry.Value)
	case SetSMove:
		extra := entry.Extra
		db.setIndex.indexes.SMove(key, string(extra), entry.Value)
	case SetSClear:
		db.setIndex.indexes.SClear(key)
	//case SetSExpire:
	//	if entry.Timestamp < uint64(time.Now().Unix()) {
	//		db.setIndex.indexes.SClear(key)
	//	} else {
	//		db.expires[Set][key] = int64(entry.Timestamp)
	//	}
	//}
}}
	// build sorted set indexes.
func (db *OpenDB) buildZsetIndex(entry *logfile.Entry){
		if db.hashIndex == nil || entry == nil {
			return
		}
		key := string(entry.Key)
		switch entry.GetType() {
		case ZSetZAdd:
			if score, err := util.StrToFloat64(string(entry.Extra)); err == nil {
				db.zsetIndex.indexes.ZAdd(key, score, string(entry.Value))
			}
		case ZSetZRem:
			db.zsetIndex.indexes.ZRem(key, string(entry.Value))
		case ZSetZClear:
			db.zsetIndex.indexes.ZClear(key)
			//case ZSetZExpire:
			//	if entry.Timestamp < uint64(time.Now().Unix()) {
			//		db.zsetIndex.indexes.ZClear(key)
			//	} else {
			//		db.expires[ZSet][key] = int64(entry.Timestamp)
			//	}
			//}
		}
}