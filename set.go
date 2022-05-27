package opendb


import (
"opendb/ds/set"
"opendb/logfile"
"sync"
"time"
)
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
	SetSClear
	SetSExpire
)
// SetIdx the set idx
type SetIdx struct {
	mu      *sync.RWMutex
	indexes *set.Set
}

// newSetIdx create new set index.
func newSetIdx() *SetIdx {
	return &SetIdx{indexes: set.New(), mu: new(sync.RWMutex)}
}

// SAdd add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
func (db *OpenDB) SAdd(key []byte, members ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, members...); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, m := range members {
		exist := db.setIndex.indexes.SIsMember(string(key), m)
		if !exist {
			e := logfile.NewEntryNoExtra(key, m, Set, SetSAdd)
			if err = db.store(e); err != nil {
				return
			}
			res = db.setIndex.indexes.SAdd(string(key), m)
		}
	}
	return
}

// SPop removes and returns one or more random members from the set value store at key.
func (db *OpenDB) SPop(key []byte, count int) (values [][]byte, err error) {
	if err = db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set) {
		return nil, ErrKeyExpired
	}

	values = db.setIndex.indexes.SPop(string(key), count)
	for _, v := range values {
		e := logfile.NewEntryNoExtra(key, v, Set, SetSRem)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// SIsMember returns if member is a member of the set stored at key.
func (db *OpenDB) SIsMember(key, member []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set) {
		return false
	}
	return db.setIndex.indexes.SIsMember(string(key), member)
}

// SRandMember returns a random element from the set value stored at key.
// count > 0: if count less than set`s card, returns an array containing count different elements. if count greater than set`s card, the entire set will be returned.
// count < 0: the command is allowed to return the same element multiple times, and in this case, the number of returned elements is the absolute value of the specified count.
func (db *OpenDB) SRandMember(key []byte, count int) [][]byte {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set) {
		return nil
	}
	return db.setIndex.indexes.SRandMember(string(key), count)
}

// SRem remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
func (db *OpenDB) SRem(key []byte, members ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, members...); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set) {
		return
	}

	for _, m := range members {
		if ok := db.setIndex.indexes.SRem(string(key), m); ok {
			e := logfile.NewEntryNoExtra(key, m, Set, SetSRem)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

// SMove move member from the set at source to the set at destination.
func (db *OpenDB) SMove(src, dst, member []byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(src, Set) {
		return ErrKeyExpired
	}
	if db.checkExpired(dst, Set) {
		return ErrKeyExpired
	}

	if ok := db.setIndex.indexes.SMove(string(src), string(dst), member); ok {
		e := logfile.NewEntry(src, member, dst, Set, SetSMove)
		if err := db.store(e); err != nil {
			return err
		}
	}
	return nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (db *OpenDB) SCard(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set) {
		return 0
	}
	return db.setIndex.indexes.SCard(string(key))
}

// SMembers returns all the members of the set value stored at key.
func (db *OpenDB) SMembers(key []byte) (val [][]byte) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set) {
		return
	}
	return db.setIndex.indexes.SMembers(string(key))
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func (db *OpenDB) SUnion(keys ...[]byte) (val [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range keys {
		if db.checkExpired(k, Set) {
			continue
		}
		validKeys = append(validKeys, string(k))
	}

	return db.setIndex.indexes.SUnion(validKeys...)
}

// SDiff returns the members of the set resulting from the difference between the first set and all the successive sets.
func (db *OpenDB) SDiff(keys ...[]byte) (val [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range keys {
		if db.checkExpired(k, Set) {
			continue
		}
		validKeys = append(validKeys, string(k))
	}

	return db.setIndex.indexes.SDiff(validKeys...)
}

// SKeyExists returns if the key exists.
func (db *OpenDB) SKeyExists(key []byte) (ok bool) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set) {
		return
	}

	ok = db.setIndex.indexes.SKeyExists(string(key))
	return
}

// SClear clear the specified key in set.
func (db *OpenDB) SClear(key []byte) (err error) {
	if !db.SKeyExists(key) {
		return ErrKeyNotExist
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	e := logfile.NewEntryNoExtra(key, nil, Set, SetSClear)
	if err = db.store(e); err != nil {
		return
	}
	db.setIndex.indexes.SClear(string(key))
	return
}

//// SExpire set expired time for the key in set.
//func (db *OpenDB) SExpire(key []byte, duration int64) (err error) {
//	if duration <= 0 {
//		return ErrInvalidTTL
//	}
//	if !db.SKeyExists(key) {
//		return ErrKeyNotExist
//	}
//
//	db.setIndex.mu.Lock()
//	defer db.setIndex.mu.Unlock()
//
//	deadline := time.Now().Unix() + duration
//	e := logfile.NewEntryWithExpire(key, nil, deadline, Set, SetSExpire)
//	if err = db.store(e); err != nil {
//		return
//	}
//	db.expires[Set][string(key)] = deadline
//	return
//}

// STTL return time to live for the key in set.
func (db *OpenDB) STTL(key []byte) (ttl int64) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set) {
		return
	}

	deadline, exist := db.expires[Set][string(key)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
