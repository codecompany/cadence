// Copyright (c) 2026 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package kvrocks

import (
	"context"
	"errors"
	"net"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"

	"github.com/redis/go-redis/v9"
)

// DB implements Cadence's NoSQL persistence interfaces backed by KVRocks (Redis protocol).
type DB struct {
	cfg    *config.NoSQL
	logger log.Logger
	dc     *persistence.DynamicConfiguration
	rdb    *redis.Client
	ns     string // key prefix / namespace (derived from cfg.Keyspace)
}

var _ nosqlplugin.DB = (*DB)(nil)
var _ nosqlplugin.AdminDB = (*DB)(nil)

func (db *DB) PluginName() string {
	return PluginName
}

func (db *DB) Close() {
	_ = db.rdb.Close()
}

func (db *DB) SetupTestDatabase(schemaBaseDir string, replicas int) error {
	// KVRocks doesn't have schemas, so just wipe DB for tests.
	return db.rdb.FlushDB(context.Background()).Err()
}

func (db *DB) TeardownTestDatabase() error {
	return db.rdb.FlushDB(context.Background()).Err()
}

func (db *DB) IsTimeoutError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

func (db *DB) IsNotFoundError(err error) bool {
	return errors.Is(err, redis.Nil)
}

func (db *DB) IsThrottlingError(err error) bool {
	// TODO(kvrocks): map BUSY/LOADING/CLUSTERDOWN style errors once implemented.
	return false
}

func (db *DB) IsDBUnavailableError(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	return false
}
