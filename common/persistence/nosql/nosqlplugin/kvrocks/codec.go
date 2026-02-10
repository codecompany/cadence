package kvrocks

import (
	"encoding/json"
	"fmt"
)

func marshalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("kvrocks: marshal json: %w", err)
	}
	return b, nil
}

func unmarshalJSON(data []byte, out any) error {
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("kvrocks: unmarshal json: %w", err)
	}
	return nil
}
