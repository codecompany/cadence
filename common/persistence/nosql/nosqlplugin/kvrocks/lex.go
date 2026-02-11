package kvrocks

import (
	"fmt"
	"strconv"
)

// encodeInt64Lex encodes an int64 as a fixed-width decimal string so lexicographic order matches numeric order.
// This only preserves ordering for non-negative values, which is fine for Cadence IDs/timestamps.
func encodeInt64Lex(v int64) string {
	return fmt.Sprintf("%019d", v)
}

func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
