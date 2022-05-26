package hash

type (
	//使用go语言的map构建一个hash表
	Hash struct {
		record Record
	}

	// 二维map map+map+[]byte
	Record map[string]map[string][]byte
)

// New create a new hash ds.
func New() *Hash {
	return &Hash{make(Record)}
}

// HSet Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
// If field already exists in the hash, it is overwritten.
func (h *Hash) HSet(key string, field string, value []byte) (res int) {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if h.record[key][field] != nil {
		// if this field exists, overwritten it.
		h.record[key][field] = value
	} else {
		// create if this field not exists.
		h.record[key][field] = value
		res = 1
	}
	return
}

// HSetNx 用于为哈希表中不存在的的字段赋值,如果哈希表不存在，一个新的哈希表被创建并进行 HSET 操作。
// 如果字段已经存在于哈希表中，操作无效。
func (h *Hash) HSetNx(key string, field string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if _, exist := h.record[key][field]; !exist {
		h.record[key][field] = value
		return 1
	}
	return 0
}

// 得到一个值
func (h *Hash) HGet(key, field string) []byte {
	if !h.exist(key) {
		return nil
	}

	return h.record[key][field]
}

//获取在哈希表中指定 key 的所有字段和值
func (h *Hash) HGetAll(key string) (res [][]byte) {
	if !h.exist(key) {
		return
	}

	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}
	return
}

// HDel removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns false.
func (h *Hash) HDel(key, field string) int {
	if !h.exist(key) {
		return 0
	}

	if _, exist := h.record[key][field]; exist {
		delete(h.record[key], field)
		return 1
	}
	return 0
}

// HKeyExists returns if key exists in hash.
func (h *Hash) HKeyExists(key string) bool {
	return h.exist(key)
}

// HExists returns if field is an existing field in the hash stored at key.
func (h *Hash) HExists(key, field string) (ok bool) {
	if !h.exist(key) {
		return
	}

	if _, exist := h.record[key][field]; exist {
		ok = true
	}
	return
}

// HLen returns the number of fields contained in the hash stored at key.
func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}
	return len(h.record[key])
}

// HKeys returns all field names in the hash stored at key.
func (h *Hash) HKeys(key string) (val []string) {
	if !h.exist(key) {
		return
	}

	for k := range h.record[key] {
		val = append(val, k)
	}
	return
}

// HVals 获取哈希表中key对应的所有值。
func (h *Hash) HVals(key string) (val [][]byte) {
	if !h.exist(key) {
		return
	}

	for _, v := range h.record[key] {
		val = append(val, v)
	}
	return
}

// HClear clear the key in hash.
func (h *Hash) HClear(key string) {
	if !h.exist(key) {
		return
	}
	delete(h.record, key)
}

func (h *Hash) exist(key string) bool {
	_, exist := h.record[key]
	return exist
}
