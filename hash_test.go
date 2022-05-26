package opendb

import (
	"testing"
)

var key = "myhash"

func TestRoseDB_HSet(t *testing.T) {
	opts := DefaultOptions("/tmp/opendb")
	db, err := Open(opts)
	if err != nil {
		t.Error(err)
	}
	db.HSet([]byte(key), []byte("my_name"), []byte("opendb"))
	getVal := func(key ,f []byte) {
		val:= db.HGet(key,f)
		if err != nil {
			t.Error("read val err: ", err)
		} else {
			t.Logf("key = %s, val = %s\n", string(key), string(val))
		}
	}

	getVal([]byte(key), []byte("my_name"))
}

