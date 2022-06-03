package logfile

import "encoding/binary"

const entryHeaderSize = 16

const (
	PUT uint16 = iota
	DEL
)

// Entry 写入文件的记录
type Entry struct {
	Key       []byte
	Value     []byte
	Extra     []byte
	KeySize   uint32
	ValueSize uint32
	ExtraSize uint32
	Mark      uint16 //对应的数据类型data type
	Type      uint16 //operation mark对应的操作.1字节
}

func NewEntry(key, value, Extra []byte, mark, Type uint16) *Entry {
	return &Entry{
		Key:       key,
		Value:     value,
		Extra:     Extra,
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		ExtraSize: uint32(len(Extra)),
		Mark:      mark,
		Type:      Type,
	}
}
func NewEntryNoExtra(key, value []byte,  mark ,Type uint16) *Entry {
	return NewEntry(key, value, nil, Type, mark)
}
func (e *Entry) GetSize() int64 {
	return int64(entryHeaderSize + e.KeySize + e.ValueSize + e.ExtraSize)
}

func (e *Entry) GetMark() uint16 {
	return uint16(e.Mark)
}
func (e *Entry) GetType() uint16 {
	return uint16(e.Type)
}
// Encode 编码 Entry，返回字节数组
func (e *Entry) Encode() ([]byte, error) {
	buf := make([]byte, e.GetSize())
	binary.BigEndian.PutUint32(buf[0:4], e.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], e.ValueSize)
	binary.BigEndian.PutUint32(buf[8:12], e.ExtraSize)
	binary.BigEndian.PutUint16(buf[12:14], e.Mark)
	binary.BigEndian.PutUint16(buf[14:16], e.Type)
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeySize], e.Key)
	copy(buf[entryHeaderSize+e.KeySize:], e.Value)
	if e.ExtraSize > 0 {
		copy(buf[(entryHeaderSize+e.KeySize+e.ValueSize):(entryHeaderSize+e.KeySize+e.ValueSize+e.ExtraSize)], e.Extra)
	}
	return buf, nil
}

// 解码 buf 字节数组，返回 Entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[0:4])
	vs := binary.BigEndian.Uint32(buf[4:8])
	es := binary.BigEndian.Uint32(buf[8:12])
	mark := binary.BigEndian.Uint16(buf[12:14])
	Type := binary.BigEndian.Uint16(buf[14:16])
	return &Entry{KeySize: ks, ValueSize: vs,ExtraSize: es,Mark: mark,Type: Type}, nil
}
