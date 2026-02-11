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
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"

	"github.com/redis/go-redis/v9"
)

const (
	// PluginName is the name used in YAML as: persistence.datastores.<name>.nosql.pluginName
	PluginName = "kvrocks"
)

type plugin struct{}

var _ nosqlplugin.Plugin = (*plugin)(nil)

func init() {
	nosql.RegisterPlugin(PluginName, &plugin{})
}

func (p *plugin) CreateDB(cfg *config.NoSQL, logger log.Logger, dc *persistence.DynamicConfiguration) (nosqlplugin.DB, error) {
	return NewDB(cfg, logger, dc)
}

func (p *plugin) CreateAdminDB(cfg *config.NoSQL, logger log.Logger, dc *persistence.DynamicConfiguration) (nosqlplugin.AdminDB, error) {
	return NewDB(cfg, logger, dc)
}

func NewDB(cfg *config.NoSQL, logger log.Logger, dc *persistence.DynamicConfiguration) (*DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kvrocks: nil config")
	}
	if dc == nil {
		return nil, fmt.Errorf("kvrocks: nil dynamic config")
	}

	addrs := parseHosts(cfg.Hosts, cfg.Port, defaultKVRocksPort)
	if len(addrs) != 1 {
		return nil, fmt.Errorf("kvrocks: expected exactly 1 endpoint in hosts, got %d: %v", len(addrs), addrs)
	}

	var tlsConfig *tls.Config
	if cfg.TLS != nil {
		var err error
		tlsConfig, err = cfg.TLS.ToTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("kvrocks: tls: %w", err)
		}
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	connectTimeout := cfg.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = 2 * time.Second
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         addrs[0],
		Username:     cfg.User,
		Password:     cfg.Password,
		DialTimeout:  connectTimeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		TLSConfig:    tlsConfig,
	})

	// Validate connectivity early so the process fails fast on misconfiguration.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("kvrocks: ping failed: %w", err)
	}

	namespace := strings.TrimSpace(cfg.Keyspace)
	if namespace == "" {
		namespace = "cadence"
	}

	return &DB{
		cfg:    cfg,
		logger: logger,
		dc:     dc,
		rdb:    rdb,
		ns:     namespace,
	}, nil
}

const defaultKVRocksPort = 6666

func parseHosts(hosts string, port int, defaultPort int) []string {
	if port == 0 {
		port = defaultPort
	}
	var out []string
	for _, h := range strings.Split(hosts, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		// If caller already provided host:port, keep it.
		if _, _, err := net.SplitHostPort(h); err == nil {
			out = append(out, h)
			continue
		}
		out = append(out, net.JoinHostPort(h, fmt.Sprintf("%d", port)))
	}
	return out
}
