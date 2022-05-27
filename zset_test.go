package opendb

import "testing"

func TestOpenDB_ZAdd(t *testing.T) {
	opts := DefaultOptions("/tmp/opendb")
	db, err := Open(opts)
	if err != nil {
		t.Error(err)
	}

	key := []byte("my_zset")
	db.ZAdd(key, 310.23, []byte("roseduan"))

	db.ZAdd(nil, 0, nil)
	db.ZAdd(key, 30.234554, []byte("Java"))
	db.ZAdd(key, 92.2233, []byte("Golang"))
	db.ZAdd(key, 221.24, []byte("Python"))
	db.ZAdd(key, 221.24, []byte("Python-tensorflow"))
	db.ZAdd(key, 221.24, []byte("Python-flask"))
	db.ZAdd(key, 221.24, []byte("Python-django"))
	db.ZAdd(key, 221.24, []byte("Python-scrapy"))
	db.ZAdd(key, 54.30003, []byte("C"))
	db.ZAdd(key, 54.30003, []byte("C plus plus"))

	if err != nil {
		t.Log(err)
	}
	ok, s := db.ZScore(key, []byte("roseduan"))
	t.Log(ok, s)
}