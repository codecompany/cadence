package kvrocks

import (
	"encoding/binary"
	"fmt"
)

func encodeFramedStringAndBytes(s string, b []byte) []byte {
	// Layout: uint32(len(s)) | s bytes | b bytes
	out := make([]byte, 4+len(s)+len(b))
	binary.BigEndian.PutUint32(out[:4], uint32(len(s)))
	copy(out[4:4+len(s)], s)
	copy(out[4+len(s):], b)
	return out
}

func decodeFramedStringAndBytes(in []byte) (string, []byte, error) {
	if len(in) < 4 {
		return "", nil, fmt.Errorf("kvrocks: framed: input too short")
	}
	n := binary.BigEndian.Uint32(in[:4])
	if int(n) < 0 || 4+int(n) > len(in) {
		return "", nil, fmt.Errorf("kvrocks: framed: invalid string length %d", n)
	}
	s := string(in[4 : 4+int(n)])
	b := in[4+int(n):]
	return s, b, nil
}
