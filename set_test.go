package opendb

import (
"testing"
)


func TestOpenDB_set(t *testing.T) {
	opts := DefaultOptions("/tmp/opendb")
	db, err := Open(opts)
	if err != nil {
		t.Error(err)
	}
	var key = []byte("my_set")
	db.SAdd([]byte(key), []byte("set_data_001"), []byte("set_data_002"), []byte("set_data_003"))
	values, _ := db.SPop(key, 3)
	for _, v := range values {
		t.Log(string(v))
	}
}
