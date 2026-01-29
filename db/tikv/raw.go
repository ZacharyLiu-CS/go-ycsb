// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/errors"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/rawkv"
)

const (
	tikvAtomicPut                  = "tikv.atomic_put"
	tikvAuthMode                   = "tikv.auth_mode"
	tikvColumnFamily               = "tikv.column_family"
	tikvReplaceUpdateWithSetKeyTTL = "tikv.replace_update_with_setkeyttl"
	tikvScanNotFillCache		   = "tikv.scan_not_fill_cache"	
)

type rawDB struct {
	db                         *rawkv.Client
	r                          *util.RowCodec
	bufPool                    *util.BufPool
	enableAuthWrite            bool
	authTokenKey               string
	authTokenValue             string
	replaceUpdateWithSetKeyTTL bool
	scanNotFillCache		   bool
}

func createRawDB(p *properties.Properties) (ycsb.DB, error) {
	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
	apiVersionStr := strings.ToUpper(p.GetString(tikvAPIVersion, "V1"))
	apiVersion, ok := kvrpcpb.APIVersion_value[apiVersionStr]
	if !ok {
		return nil, errors.Errorf("Invalid tikv apiversion %s.", apiVersionStr)
	}
	db, err := rawkv.NewClientWithOpts(context.Background(), strings.Split(pdAddr, ","),
		rawkv.WithAPIVersion(kvrpcpb.APIVersion(apiVersion)))

	// set column family
	cf := p.GetString(tikvColumnFamily, "default")
	db.SetColumnFamily(cf)

	if err != nil {
		return nil, err
	}
	if p.GetString(tikvAtomicPut, "false") == "true" {
		log.Println("Set atomic for cas true")
		db.SetAtomicForCAS(true)
	}

	// enable authorization for write
	enableAuthWrite := false
	authTokenKey := "r_ycsb_auth_k"
	authTokenValue := "ycsb_auth_v"
	if p.GetString(tikvAuthMode, "false") == "true" {
		log.Println("Set auth mode for put, delete and write")
		enableAuthWrite = true
		db.Put(context.Background(), []byte(authTokenKey), []byte(authTokenValue))
	}

	// enable scan not fill cache
	scanNotFillCache := p.GetString(tikvScanNotFillCache, "false") == "true"
	if scanNotFillCache {
		log.Println("Set scan not fill cache mode")
	}

	bufPool := util.NewBufPool()

	// enable replace update with SetKeyTTL
	replaceUpdateWithSetKeyTTL := p.GetString(tikvReplaceUpdateWithSetKeyTTL, "false") == "true"
	if replaceUpdateWithSetKeyTTL {
		log.Println("Set replace update with SetKeyTTL mode")
	}

	return &rawDB{
		db:                         db,
		r:                          util.NewRowCodec(p),
		bufPool:                    bufPool,
		enableAuthWrite:            enableAuthWrite,
		authTokenKey:               authTokenKey,
		authTokenValue:             authTokenValue,
		replaceUpdateWithSetKeyTTL: replaceUpdateWithSetKeyTTL,
		scanNotFillCache:		   	scanNotFillCache,
	}, nil
}

func (db *rawDB) Close() error {
	return db.db.Close()
}

func (db *rawDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *rawDB) CleanupThread(ctx context.Context) {
}

func (db *rawDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *rawDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	row, err := db.db.Get(ctx, db.getRowKey(table, key))
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	return db.r.Decode(row, fields)
}

func (db *rawDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	values, err := db.db.BatchGet(ctx, rowKeys)
	if err != nil {
		return nil, err
	}
	rowValues := make([]map[string][]byte, len(keys))

	for i, value := range values {
		if len(value) > 0 {
			rowValues[i], err = db.r.Decode(value, fields)
		} else {
			rowValues[i] = nil
		}
	}
	return rowValues, nil
}

func (db *rawDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var opts []rawkv.RawOption
	if db.scanNotFillCache {
		opts = append(opts, rawkv.ScanNotFillCache(db.scanNotFillCache))
	} 
	_, rows, err := db.db.Scan(ctx, db.getRowKey(table, startKey), nil, count, opts...)
	if err != nil {
		return nil, err
	}

	res := make([]map[string][]byte, len(rows))
	for i, row := range rows {
		if row == nil {
			res[i] = nil
			continue
		}

		v, err := db.r.Decode(row, fields)
		if err != nil {
			return nil, err
		}
		res[i] = v
	}

	return res, nil
}

func (db *rawDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	row, err := db.db.Get(ctx, db.getRowKey(table, key))
	if err != nil {
		return nil
	}

	data, err := db.r.Decode(row, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		data[field] = value
	}

	// Update data and use Insert to overwrite.
	if db.replaceUpdateWithSetKeyTTL {
		// When replaceUpdateWithSetKeyTTL is enabled, use SetKeyTTL instead of Put
		// For now, we use a default TTL of 0 (no TTL) since the original Update doesn't have TTL
		rowKey := db.getRowKey(table, key)
		var opts []rawkv.RawOption
		if db.enableAuthWrite {
			opts = append(opts, rawkv.SetWriteAuthToken(db.authTokenKey, db.authTokenValue))
		}
		return db.SetKeyTTL(ctx, rowKey, 0, opts...)
	}

	return db.Insert(ctx, table, key, data)
}

func (db *rawDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		// TODO should we check the key exist?
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}

	if db.replaceUpdateWithSetKeyTTL {
		// When replaceUpdateWithSetKeyTTL is enabled, use BatchSetKeyTTL instead of BatchPut
		// For now, we use a default TTL of 0 (no TTL) for all keys since the original BatchUpdate doesn't have TTL
		ttls := make([]uint64, len(rawKeys))
		for i := range ttls {
			ttls[i] = 0
		}

		var opts []rawkv.RawOption
		if db.enableAuthWrite {
			opts = append(opts, rawkv.SetWriteAuthToken(db.authTokenKey, db.authTokenValue))
		}
		return db.BatchSetKeyTTL(ctx, rawKeys, ttls, opts...)
	}

	if db.enableAuthWrite {
		return db.db.BatchPut(ctx, rawKeys, rawValues, rawkv.SetWriteAuthToken(db.authTokenKey, db.authTokenValue))
	}
	return db.db.BatchPut(ctx, rawKeys, rawValues)
}

func (db *rawDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}
	if db.enableAuthWrite {
		return db.db.Put(ctx, db.getRowKey(table, key), buf, rawkv.SetWriteAuthToken(db.authTokenKey, db.authTokenValue))
	}
	return db.db.Put(ctx, db.getRowKey(table, key), buf)
}

func (db *rawDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}
	if db.enableAuthWrite {
		return db.db.BatchPut(ctx, rawKeys, rawValues, rawkv.SetWriteAuthToken(db.authTokenKey, db.authTokenValue))
	}
	return db.db.BatchPut(ctx, rawKeys, rawValues)
}

func (db *rawDB) Delete(ctx context.Context, table string, key string) error {
	if db.enableAuthWrite {
		return db.db.Delete(ctx, db.getRowKey(table, key), rawkv.SetWriteAuthToken(db.authTokenKey, db.authTokenValue))
	}
	return db.db.Delete(ctx, db.getRowKey(table, key))
}

func (db *rawDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	if db.enableAuthWrite {
		return db.db.BatchDelete(ctx, rowKeys, rawkv.SetWriteAuthToken(db.authTokenKey, db.authTokenValue))
	}
	return db.db.BatchDelete(ctx, rowKeys)
}

// SetKeyTTL updates the TTL of an existing raw key without changing its value
func (db *rawDB) SetKeyTTL(ctx context.Context, key []byte, ttl uint64, options ...rawkv.RawOption) error {
	// Put the same value back (TTL functionality will be added later)
	return db.db.SetKeyTTL(ctx, key, ttl, options...)
}

// BatchSetKeyTTL batch updates TTLs for multiple raw keys
func (db *rawDB) BatchSetKeyTTL(ctx context.Context, keys [][]byte, ttls []uint64, options ...rawkv.RawOption) error {
	if len(keys) != len(ttls) {
		return errors.New("length of keys must equal length of ttls")
	}

	// Put the same values back (TTL functionality will be added later)
	return db.db.BatchSetKeyTTL(ctx, keys, ttls, options...)
}
