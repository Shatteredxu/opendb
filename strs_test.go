package rosedb

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestRoseDB_Set(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBSet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBSet(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBSet(t, FileIO, KeyValueMemMode)
	})
}

func TestRoseDB_Set_LogFileThreshold(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = MMap
	opts.LogFileSizeThreshold = 32 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 600000; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}
}

func TestRoseDB_Get(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBGet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBGet(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBGet(t, MMap, KeyValueMemMode)
	})
}

func TestRoseDB_Get_LogFileThreshold(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = MMap
	opts.LogFileSizeThreshold = 32 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	writeCount := 600000
	for i := 0; i <= writeCount; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	rand.Seed(time.Now().Unix())
	for i := 0; i < 10000; i++ {
		key := GetKey(rand.Intn(writeCount))
		v, err := db.Get(key)
		assert.Nil(t, err)
		assert.NotNil(t, v)
	}
}

func testRoseDBSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		wantErr bool
	}{
		{
			"nil-key", db, args{key: nil, value: []byte("val-1")}, false,
		},
		{
			"nil-value", db, args{key: []byte("key-1"), value: nil}, false,
		},
		{
			"normal", db, args{key: []byte("key-1111"), value: []byte("value-1111")}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.Set(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func testRoseDBGet(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	db.Set(nil, []byte("v-1111"))
	db.Set([]byte("k-1"), []byte("v-1"))
	db.Set([]byte("k-2"), []byte("v-2"))
	db.Set([]byte("k-3"), []byte("v-3"))
	db.Set([]byte("k-3"), []byte("v-333"))

	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"nil-key", db, args{key: nil}, nil, true,
		},
		{
			"normal", db, args{key: []byte("k-1")}, []byte("v-1"), false,
		},
		{
			"normal-rewrite", db, args{key: []byte("k-3")}, []byte("v-333"), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRoseDB_MGet(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBMGet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBMGet(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBMGet(t, MMap, KeyValueMemMode)
	})

}

func testRoseDBMGet(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	db.Set(nil, []byte("v-1111"))
	db.Set([]byte("k-1"), []byte("v-1"))
	db.Set([]byte("k-2"), []byte("v-2"))
	db.Set([]byte("k-3"), []byte("v-3"))
	db.Set([]byte("k-3"), []byte("v-333"))
	db.Set([]byte("k-4"), []byte("v-4"))
	db.Set([]byte("k-5"), []byte("v-5"))

	type args struct {
		keys [][]byte
	}

	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		want    [][]byte
		wantErr bool
	}{
		{
			name:    "nil-key",
			db:      db,
			args:    args{keys: [][]byte{nil}},
			want:    [][]byte{nil},
			wantErr: false,
		},
		{
			name:    "normal",
			db:      db,
			args:    args{keys: [][]byte{[]byte("k-1")}},
			want:    [][]byte{[]byte("v-1")},
			wantErr: false,
		},
		{
			name:    "normal-rewrite",
			db:      db,
			args:    args{keys: [][]byte{[]byte("k-1"), []byte("k-3")}},
			want:    [][]byte{[]byte("v-1"), []byte("v-333")},
			wantErr: false,
		},
		{
			name: "multiple key",
			db:   db,
			args: args{keys: [][]byte{
				[]byte("k-1"),
				[]byte("k-2"),
				[]byte("k-4"),
				[]byte("k-5"),
			}},
			want: [][]byte{
				[]byte("v-1"),
				[]byte("v-2"),
				[]byte("v-4"),
				[]byte("v-5"),
			},
			wantErr: false,
		},
		{
			name:    "missed one key",
			db:      db,
			args:    args{keys: [][]byte{[]byte("missed-k")}},
			want:    [][]byte{nil},
			wantErr: false,
		},
		{
			name: "missed multiple keys",
			db:   db,
			args: args{keys: [][]byte{
				[]byte("missed-k-1"),
				[]byte("missed-k-2"),
				[]byte("missed-k-3"),
			}},
			want:    [][]byte{nil, nil, nil},
			wantErr: false,
		},
		{
			name: "missed one key in multiple keys",
			db:   db,
			args: args{keys: [][]byte{
				[]byte("k-1"),
				[]byte("missed-k-1"),
				[]byte("k-2"),
			}},
			want:    [][]byte{[]byte("v-1"), nil, []byte("v-2")},
			wantErr: false,
		},
		{
			name:    "nil key in multiple keys",
			db:      db,
			args:    args{keys: [][]byte{nil, []byte("k-1")}},
			want:    [][]byte{nil, []byte("v-1")},
			wantErr: false,
		},
		{
			name:    "empty key",
			db:      db,
			args:    args{keys: [][]byte{}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.MGet(tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRoseDB_Delete(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBDelete(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBDelete(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBDelete(t, MMap, KeyValueMemMode)
	})
}

func TestRoseDB_Delete_MultiFiles(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = FileIO
	opts.LogFileSizeThreshold = 32 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	writeCount := 600000
	for i := 0; i <= writeCount; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	var deletedKeys [][]byte
	rand.Seed(time.Now().Unix())
	for i := 0; i < 10000; i++ {
		key := GetKey(rand.Intn(writeCount))
		err := db.Delete(key)
		assert.Nil(t, err)
		deletedKeys = append(deletedKeys, key)
	}
	for _, k := range deletedKeys {
		_, err := db.Get(k)
		assert.Equal(t, ErrKeyNotFound, err)
	}
}

func testRoseDBDelete(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	db.Set(nil, []byte("v-1111"))
	db.Set([]byte("k-1"), []byte("v-1"))
	db.Set([]byte("k-3"), []byte("v-3"))
	db.Set([]byte("k-3"), []byte("v-333"))

	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		wantErr bool
	}{
		{
			"nil", db, args{key: nil}, false,
		},
		{
			"normal-1", db, args{key: []byte("k-1")}, false,
		},
		{
			"normal-2", db, args{key: []byte("k-3")}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRoseDB_SetEx(t *testing.T) {
	t.Run("key-only", func(t *testing.T) {
		testRoseDBSetEx(t, KeyOnlyMemMode)
	})

	t.Run("key-value", func(t *testing.T) {
		testRoseDBSetEx(t, KeyValueMemMode)
	})
}

func testRoseDBSetEx(t *testing.T, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	err = db.SetEX(GetKey(1), GetValue16B(), time.Millisecond*200)
	assert.Nil(t, err)
	time.Sleep(time.Millisecond * 205)
	v, err := db.Get(GetKey(1))
	assert.Equal(t, 0, len(v))
	assert.Equal(t, ErrKeyNotFound, err)

	err = db.SetEX(GetKey(2), GetValue16B(), time.Second*200)
	time.Sleep(time.Millisecond * 200)
	v1, err := db.Get(GetKey(2))
	assert.NotNil(t, v1)
	assert.Nil(t, err)

	// set an existed key.
	err = db.Set(GetKey(3), GetValue16B())
	assert.Nil(t, err)

	err = db.SetEX(GetKey(3), GetValue16B(), time.Millisecond*200)
	time.Sleep(time.Millisecond * 205)
	v2, err := db.Get(GetKey(3))
	assert.Equal(t, 0, len(v2))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestRoseDB_SetNX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBSetNX(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBSetNX(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBSetNX(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBSetNX(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key     []byte
		value   []byte
		wantErr bool
	}
	tests := []struct {
		name string
		db   *RoseDB
		args []args
	}{
		{
			name: "nil-key",
			db:   db,
			args: []args{{key: nil, value: []byte("val-1")}},
		},
		{
			name: "nil-value",
			db:   db,
			args: []args{{key: []byte("key-1"), value: nil}},
		},
		{
			name: "not exist in db",
			db:   db,
			args: []args{
				{
					key:     []byte("key-1"),
					value:   []byte("val-1"),
					wantErr: false,
				},
			},
		},
		{
			name: "exist in db",
			db:   db,
			args: []args{
				{
					key:     []byte("key-1"),
					value:   []byte("val-1"),
					wantErr: false,
				},
				{
					key:     []byte("key-1"),
					value:   []byte("val-1"),
					wantErr: false,
				},
			},
		},
		{
			name: "not exist in multiple valued db",
			db:   db,
			args: []args{
				{
					key:     []byte("key-1"),
					value:   []byte("value-1"),
					wantErr: false,
				},
				{
					key:     []byte("key-2"),
					value:   []byte("value-2"),
					wantErr: false,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, arg := range tt.args {
				if err := tt.db.SetNX(arg.key, arg.value); (err != nil) != arg.wantErr {
					t.Errorf("Set() error = %v, wantErr %v", err, arg.wantErr)
				}
			}
		})
	}
}

func TestRoseDB_MSet(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBMSet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBMSet(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBMSet(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBMSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	tests := []struct {
		name    string
		db      *RoseDB
		args    [][]byte
		wantErr bool
	}{
		{
			name:    "nil-key",
			db:      db,
			args:    [][]byte{nil, []byte("val-1")},
			wantErr: false,
		},
		{
			name:    "nil-value",
			db:      db,
			args:    [][]byte{[]byte("key-1"), nil},
			wantErr: false,
		},
		{
			name:    "empty pair",
			db:      db,
			args:    [][]byte{},
			wantErr: true,
		},
		{
			name:    "one pair",
			db:      db,
			args:    [][]byte{[]byte("key-1"), []byte("value-1")},
			wantErr: false,
		},
		{
			name: "multiple pair",
			db:   db,
			args: [][]byte{
				[]byte("key-1"), []byte("value-1"),
				[]byte("key-2"), []byte("value-2"),
				[]byte("key-3"), []byte("value-3"),
			},
			wantErr: false,
		},
		{
			name: "wrong number of key-value",
			db:   db,
			args: [][]byte{
				[]byte("key-1"), []byte("value-1"),
				[]byte("key-2"), []byte("value-2"),
				[]byte("key-3"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.MSet(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr == true && !errors.Is(err, ErrWrongNumberOfArgs) {
				t.Errorf("Set() error = %v, expected error = %v", err, ErrWrongNumberOfArgs)
			}
		})
	}
}

func TestRoseDB_Append(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBAppend(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBAppend(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBAppend(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBAppend(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		wantErr bool
	}{
		{
			"nil-key", db, args{key: nil, value: []byte("val-1")}, false,
		},
		{
			"nil-value", db, args{key: []byte("key-1"), value: nil}, false,
		},
		{
			"not exist in db", db, args{key: []byte("key-2"), value: []byte("val-2")}, false,
		},
		{
			"exist in db", db, args{key: []byte("key-2"), value: []byte("val-2")}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.Append(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRoseDB_MSetNX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBMSetNX(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBMSetNX(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBMSetNX(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBMSetNX(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.Set([]byte("key-10"), []byte("value-10"))
	tests := []struct {
		name            string
		db              *RoseDB
		args            [][]byte
		expDuplicateKey []byte
		expDuplicateVal []byte
		wantErr         bool
	}{
		{
			name:    "nil-key",
			db:      db,
			args:    [][]byte{nil, []byte("val-1")},
			wantErr: false,
		},
		{
			name:    "nil-value",
			db:      db,
			args:    [][]byte{[]byte("key-1"), nil},
			wantErr: false,
		},
		{
			name:    "empty pair",
			db:      db,
			args:    [][]byte{},
			wantErr: true,
		},
		{
			name:    "one pair",
			db:      db,
			args:    [][]byte{[]byte("key-1"), []byte("value-1")},
			wantErr: false,
		},
		{
			name: "multiple pair - no duplicate",
			db:   db,
			args: [][]byte{
				[]byte("key-1"), []byte("value-1"),
				[]byte("key-2"), []byte("value-2"),
				[]byte("key-3"), []byte("value-3"),
			},
			wantErr: false,
		},
		{
			name: "multiple pair - duplicate exists",
			db:   db,
			args: [][]byte{
				[]byte("key-11"), []byte("value-1"),
				[]byte("key-12"), []byte("value-2"),
				[]byte("key-12"), []byte("value-3")},
			expDuplicateKey: []byte("key-12"),
			expDuplicateVal: []byte("value-2"),
			wantErr:         false,
		},
		{
			name: "multiple pair - already exists",
			db:   db,
			args: [][]byte{
				[]byte("key-1"), []byte("value-1"),
				[]byte("key-2"), []byte("value-2"),
				[]byte("key-10"), []byte("value-20"),
			},
			expDuplicateKey: []byte("key-10"),
			expDuplicateVal: []byte("value-10"),
			wantErr:         false,
		},
		{
			name: "wrong number of key-value",
			db:   db,
			args: [][]byte{
				[]byte("key-1"), []byte("value-1"),
				[]byte("key-2"), []byte("value-2"),
				[]byte("key-3"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = tt.db.MSetNX(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSetNX() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr == true && !errors.Is(err, ErrWrongNumberOfArgs) {
				t.Errorf("MSetNX() error = %v, expected error = %v", err, ErrWrongNumberOfArgs)
			}
			if tt.expDuplicateVal != nil {
				val, _ := tt.db.Get(tt.expDuplicateKey)
				if !bytes.Equal(val, tt.expDuplicateVal) {
					t.Errorf("expected duplicate value = %v, got = %v", string(tt.expDuplicateVal), string(val))
				}
			}
		})
	}
}

func TestRoseDB_Decr(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBDecr(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBDecr(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBDecr(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBDecr(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.MSet([]byte("nil-value"), nil,
		[]byte("ten"), []byte("10"),
		[]byte("min"), []byte(strconv.Itoa(math.MinInt64)),
		[]byte("str-key"), []byte("str-val"))
	tests := []struct {
		name    string
		db      *RoseDB
		key     []byte
		expVal  int64
		expByte []byte
		expErr  error
		wantErr bool
	}{
		{
			name:    "nil value",
			db:      db,
			key:     []byte("nil-value"),
			expVal:  -1,
			expByte: []byte("-1"),
			wantErr: false,
		},
		{
			name:    "exist key",
			db:      db,
			key:     []byte("ten"),
			expVal:  9,
			expByte: []byte("9"),
			wantErr: false,
		},
		{
			name:    "non-exist key",
			db:      db,
			key:     []byte("zero"),
			expVal:  -1,
			expByte: []byte("-1"),
			wantErr: false,
		},
		{
			name:    "overflow value",
			db:      db,
			key:     []byte("min"),
			expVal:  0,
			expByte: []byte(strconv.Itoa(math.MinInt64)),
			expErr:  ErrIntegerOverflow,
			wantErr: true,
		},
		{
			name:    "wrong type",
			db:      db,
			key:     []byte("str-key"),
			expVal:  0,
			expErr:  ErrWrongValueType,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newVal, err := tt.db.Decr(tt.key)
			if (err != nil) != tt.wantErr || err != tt.expErr {
				t.Errorf("Decr() error = %v, wantErr = %v", err, tt.expErr)
			}
			if newVal != tt.expVal {
				t.Errorf("Decr() expected value = %v, actual value = %v", tt.expVal, newVal)
			}
			val, _ := tt.db.Get(tt.key)
			if tt.expByte != nil && !bytes.Equal(val, tt.expByte) {
				t.Errorf("Decr() expected value = %v, actual = %v", tt.expByte, val)
			}
		})
	}
}

func TestRoseDB_DecrBy(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBDecrBy(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBDecrBy(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBDecrBy(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBDecrBy(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.MSet([]byte("nil-value"), nil,
		[]byte("ten"), []byte("10"),
		[]byte("min"), []byte(strconv.Itoa(math.MinInt64)),
		[]byte("max"), []byte(strconv.Itoa(math.MaxInt64)),
		[]byte("str-key"), []byte("str-val"),
		[]byte("neg"), []byte("11"))
	tests := []struct {
		name    string
		db      *RoseDB
		key     []byte
		decr    int64
		expVal  int64
		expByte []byte
		expErr  error
		wantErr bool
	}{
		{
			name:    "nil value",
			db:      db,
			key:     []byte("nil-value"),
			decr:    10,
			expVal:  -10,
			expByte: []byte("-10"),
			wantErr: false,
		},
		{
			name:    "exist key",
			db:      db,
			key:     []byte("ten"),
			decr:    25,
			expVal:  -15,
			expByte: []byte("-15"),
			wantErr: false,
		},
		{
			name:    "non-exist key",
			db:      db,
			key:     []byte("zero"),
			decr:    3,
			expVal:  -3,
			expByte: []byte("-3"),
			wantErr: false,
		},
		{
			name:    "overflow value-min",
			db:      db,
			key:     []byte("min"),
			decr:    3,
			expVal:  0,
			expByte: []byte(strconv.Itoa(math.MinInt64)),
			expErr:  ErrIntegerOverflow,
			wantErr: true,
		},
		{
			name:    "overflow value-max",
			db:      db,
			key:     []byte("max"),
			decr:    -10,
			expVal:  0,
			expByte: []byte(strconv.Itoa(math.MaxInt64)),
			expErr:  ErrIntegerOverflow,
			wantErr: true,
		},
		{
			name:    "wrong type",
			db:      db,
			key:     []byte("str-key"),
			decr:    5,
			expVal:  0,
			expErr:  ErrWrongValueType,
			wantErr: true,
		},
		{
			name:    "negative incr",
			db:      db,
			key:     []byte("neg"),
			decr:    -4,
			expVal:  15,
			expByte: []byte("15"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newVal, err := tt.db.DecrBy(tt.key, tt.decr)
			if (err != nil) != tt.wantErr || err != tt.expErr {
				t.Errorf("DecrBy() error = %v, wantErr = %v", err, tt.expErr)
			}
			if newVal != tt.expVal {
				t.Errorf("DecrBy() expected value = %v, actual value = %v", tt.expVal, newVal)
			}
			val, _ := tt.db.Get(tt.key)
			if tt.expByte != nil && !bytes.Equal(val, tt.expByte) {
				t.Errorf("DecrBy() expected value = %v, actual = %v", tt.expByte, val)
			}
		})
	}
}

func TestRoseDB_Incr(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBIncr(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBIncr(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBIncr(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBIncr(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.MSet([]byte("nil-value"), nil,
		[]byte("ten"), []byte("10"),
		[]byte("max"), []byte(strconv.Itoa(math.MaxInt64)),
		[]byte("str-key"), []byte("str-val"))
	tests := []struct {
		name    string
		db      *RoseDB
		key     []byte
		expVal  int64
		expByte []byte
		expErr  error
		wantErr bool
	}{
		{
			name:    "nil value",
			db:      db,
			key:     []byte("nil-value"),
			expVal:  1,
			expByte: []byte("1"),
			wantErr: false,
		},
		{
			name:    "exist key",
			db:      db,
			key:     []byte("ten"),
			expVal:  11,
			expByte: []byte("11"),
			wantErr: false,
		},
		{
			name:    "non-exist key",
			db:      db,
			key:     []byte("zero"),
			expVal:  1,
			expByte: []byte("1"),
			wantErr: false,
		},
		{
			name:    "overflow value-max",
			db:      db,
			key:     []byte("max"),
			expVal:  0,
			expByte: []byte(strconv.Itoa(math.MaxInt64)),
			expErr:  ErrIntegerOverflow,
			wantErr: true,
		},
		{
			name:    "wrong type",
			db:      db,
			key:     []byte("str-key"),
			expVal:  0,
			expErr:  ErrWrongValueType,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newVal, err := tt.db.Incr(tt.key)
			if (err != nil) != tt.wantErr || err != tt.expErr {
				t.Errorf("Incr() error = %v, wantErr = %v", err, tt.expErr)
			}
			if newVal != tt.expVal {
				t.Errorf("Incr() expected value = %v, actual value = %v", tt.expVal, newVal)
			}
			val, _ := tt.db.Get(tt.key)
			if tt.expByte != nil && !bytes.Equal(val, tt.expByte) {
				t.Errorf("Incr() expected value = %v, actual = %v", tt.expByte, val)
			}
		})
	}
}

func TestRoseDB_IncrBy(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBIncrBy(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBIncrBy(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBIncrBy(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBIncrBy(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.MSet([]byte("nil-value"), nil,
		[]byte("ten"), []byte("10"),
		[]byte("min"), []byte(strconv.Itoa(math.MinInt64)),
		[]byte("max"), []byte(strconv.Itoa(math.MaxInt64)),
		[]byte("str-key"), []byte("str-val"),
		[]byte("neg"), []byte("11"))
	tests := []struct {
		name    string
		db      *RoseDB
		key     []byte
		incr    int64
		expVal  int64
		expByte []byte
		expErr  error
		wantErr bool
	}{
		{
			name:    "nil value",
			db:      db,
			key:     []byte("nil-value"),
			incr:    10,
			expVal:  10,
			expByte: []byte("10"),
			wantErr: false,
		},
		{
			name:    "exist key",
			db:      db,
			key:     []byte("ten"),
			incr:    25,
			expVal:  35,
			expByte: []byte("35"),
			wantErr: false,
		},
		{
			name:    "non-exist key",
			db:      db,
			key:     []byte("zero"),
			incr:    3,
			expVal:  3,
			expByte: []byte("3"),
			wantErr: false,
		},
		{
			name:    "overflow value-min",
			db:      db,
			key:     []byte("min"),
			incr:    -3,
			expVal:  0,
			expByte: []byte(strconv.Itoa(math.MinInt64)),
			expErr:  ErrIntegerOverflow,
			wantErr: true,
		},
		{
			name:    "overflow value-max",
			db:      db,
			key:     []byte("max"),
			incr:    10,
			expVal:  0,
			expByte: []byte(strconv.Itoa(math.MaxInt64)),
			expErr:  ErrIntegerOverflow,
			wantErr: true,
		},
		{
			name:    "wrong type",
			db:      db,
			key:     []byte("str-key"),
			incr:    5,
			expVal:  0,
			expErr:  ErrWrongValueType,
			wantErr: true,
		},
		{
			name:    "negative incr",
			db:      db,
			key:     []byte("neg"),
			incr:    -4,
			expVal:  7,
			expByte: []byte("7"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newVal, err := tt.db.IncrBy(tt.key, tt.incr)
			if (err != nil) != tt.wantErr || err != tt.expErr {
				t.Errorf("IncrBy() error = %v, wantErr = %v", err, tt.expErr)
			}
			if newVal != tt.expVal {
				t.Errorf("IncrBy() expected value = %v, actual value = %v", tt.expVal, newVal)
			}
			val, _ := tt.db.Get(tt.key)
			if tt.expByte != nil && !bytes.Equal(val, tt.expByte) {
				t.Errorf("IncrBy() expected value = %v, actual = %v", tt.expByte, val)
			}
		})
	}
}

func TestRoseDB_StrLen(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBStrLen(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBStrLen(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBStrLen(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBStrLen(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.MSet([]byte("string"), []byte("value"), []byte("empty"), []byte(""))

	tests := []struct {
		name   string
		db     *RoseDB
		key    []byte
		expLen int
	}{
		{
			name:   "Empty",
			db:     db,
			key:    []byte("empty"),
			expLen: 0,
		},
		{
			name:   "not exist",
			db:     db,
			key:    []byte("not-exist-key"),
			expLen: 0,
		},
		{
			name:   "normal string",
			db:     db,
			key:    []byte("string"),
			expLen: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strLen := tt.db.StrLen(tt.key)
			if strLen != tt.expLen {
				t.Errorf("StrLen() expected length = %v, actual length = %v", tt.expLen, strLen)
			}
		})
	}
}

func TestRoseDB_GetDel(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRoseDBGetDel(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBGetDel(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRoseDBGetDel(t, FileIO, KeyValueMemMode)
	})
}

func testRoseDBGetDel(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.MSet(
		[]byte("nil-value"), nil,
		[]byte("key-1"), []byte("value-1"),
		[]byte("key-2"), []byte("value-2"),
		[]byte("key-3"), []byte("value-3"),
		[]byte("key-4"), []byte("value-4"),
	)
	tests := []struct {
		name   string
		db     *RoseDB
		key    []byte
		expVal []byte
		expErr error
	}{
		{
			name:   "nil value",
			db:     db,
			key:    []byte("nil-value"),
			expVal: nil,
			expErr: nil,
		},
		{
			name:   "not exist in db",
			db:     db,
			key:    []byte("not-exist-key"),
			expVal: nil,
			expErr: nil,
		},
		{
			name:   "exist in db",
			db:     db,
			key:    []byte("key-1"),
			expVal: []byte("value-1"),
			expErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := tt.db.GetDel(tt.key)
			if err != tt.expErr {
				t.Errorf("GetDel(): expected error: %+v, actual error: %+v", tt.expErr, err)
			}
			if !bytes.Equal(val, tt.expVal) {
				t.Errorf("GetDel(): expected val: %v, actual val: %v", tt.expVal, val)
			}

			val, _ = tt.db.Get(tt.key)
			if val != nil {
				t.Errorf("GetDel(): expected val(after Get()): <nil>, actual val(after Get()): %v", val)
			}
		})
	}
}

func TestRoseDB_DiscardStat_Strs(t *testing.T) {
	helper := func(isDelete bool) {
		path := filepath.Join("/tmp", "rosedb")
		opts := DefaultOptions(path)
		opts.LogFileSizeThreshold = 64 << 20
		db, err := Open(opts)
		assert.Nil(t, err)
		defer destroyDB(db)

		writeCount := 500000
		for i := 0; i < writeCount/2; i++ {
			err := db.Set(GetKey(i), GetValue128B())
			assert.Nil(t, err)
		}

		if isDelete {
			for i := 0; i < writeCount/2; i++ {
				err := db.Delete(GetKey(i))
				assert.Nil(t, err)
			}
		} else {
			for i := 0; i < writeCount/2; i++ {
				err := db.Set(GetKey(i), GetValue128B())
				assert.Nil(t, err)
			}
		}
		_ = db.Sync()
		ccl, err := db.discards[String].getCCL(10, 0.5)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(ccl))
	}

	t.Run("rewrite", func(t *testing.T) {
		helper(false)
	})

	t.Run("delete", func(t *testing.T) {
		helper(true)
	})
}

func TestRoseDB_StrsGC(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 64 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	writeCount := 1000000
	for i := 0; i < writeCount; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}
	for i := 0; i < writeCount/4; i++ {
		err := db.Delete(GetKey(i))
		assert.Nil(t, err)
	}

	err = db.RunLogFileGC(String, 0, 0.6)
	assert.Nil(t, err)
	size := db.strIndex.idxTree.Size()
	assert.Equal(t, writeCount-writeCount/4, size)
}
