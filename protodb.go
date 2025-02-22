// Copyright 2021 Linka Cloud  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protodb

import (
	"context"

	"go.linka.cloud/protofilters/filters"

	"go.linka.cloud/protodb/internal/badgerd/replication"
	"go.linka.cloud/protodb/internal/client"
	"go.linka.cloud/protodb/internal/db"
	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/internal/server"
	"go.linka.cloud/protodb/pb"
)

var (
	Where = filters.Where
)

type DB = protodb.DB

type Tx = protodb.Tx

type Reader = protodb.Reader

type Watcher = protodb.Watcher

type Writer protodb.Writer

type TxProvider = protodb.TxProvider

type Committer = protodb.Committer

type Registerer = protodb.Registerer

type Leader = protodb.Leader

type Resolverer = protodb.Resolverer

type EventType = pb.WatchEventType

const (
	EventTypeEnter  = pb.WatchEventEnter
	EventTypeLeave  = pb.WatchEventLeave
	EventTypeUpdate = pb.WatchEventUpdate
)

type Event = protodb.Event

var DefaultPath = db.DefaultPath

var Open = db.Open

type Option = db.Option

var (
	WithPath                      = db.WithPath
	WithInMemory                  = db.WithInMemory
	WithBadgerOptionsFunc         = db.WithBadgerOptionsFunc
	WithLogger                    = db.WithLogger
	WithApplyDefaults             = db.WithApplyDefaults
	WithIgnoreProtoRegisterErrors = db.WithIgnoreProtoRegisterErrors
	WithProtoRegisterErrHandler   = db.WithProtoRegisterErrHandler
	WithOnClose                   = db.WithOnClose
	WithReplication               = db.WithReplication
)

type GetOption = protodb.GetOption

var (
	WithPaging             = protodb.WithPaging
	WithFilter             = protodb.WithFilter
	WithReadFieldMaskPaths = protodb.WithReadFieldMaskPaths
	WithReadFieldMask      = protodb.WithReadFieldMask
	WithReverse            = protodb.WithReverse
	WithOne                = protodb.WithOne
)

type TxOption = protodb.TxOption

var WithReadOnly = protodb.WithReadOnly

type SetOption = protodb.SetOption

var (
	WithTTL                 = protodb.WithTTL
	WithWriteFieldMaskPaths = protodb.WithWriteFieldMaskPaths
	WithWriteFieldMask      = protodb.WithWriteFieldMask
)

type ReplicationMode = replication.Mode

var (
	ReplicationModeNone  = replication.ModeNone
	ReplicationModeAsync = replication.ModeAsync
	ReplicationModeSync  = replication.ModeSync
)

type ReplicationOption = replication.Option

var (
	WithMode       = replication.WithMode
	WithName       = replication.WithName
	WithAddrs      = replication.WithAddrs
	WithGossipPort = replication.WithGossipPort
	WithGRPCPort   = replication.WithGRPCPort
	WithTick       = replication.WithTick
	// WithEncryptionKey set a key to encrypt gossip messages
	WithEncryptionKey = replication.WithEncryptionKey
	WithServerCert    = replication.WithServerCert
	WithServerKey     = replication.WithServerKey
	WithClientCert    = replication.WithClientCert
	WithClientKey     = replication.WithClientKey
	WithClientCA      = replication.WithClientCA
	WithTLSConfig     = replication.WithTLSConfig
)

type (
	Paging     = pb.Paging
	PagingInfo = pb.PagingInfo
	FilterExpr = filters.Expression
	Filter     = filters.FieldFilterer
)

type Client = client.Client

var NewClient = client.NewClient

type Server = server.Server

var NewServer = server.NewServer

func WithTx(ctx context.Context, db TxProvider, fn func(ctx context.Context, tx Tx) error, opts ...TxOption) error {
	tx, err := db.Tx(ctx, opts...)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func WithTx2[R any](ctx context.Context, db TxProvider, fn func(ctx context.Context, tx Tx) (R, error), opts ...TxOption) (R, error) {
	var o R
	tx, err := db.Tx(ctx, opts...)
	if err != nil {
		return o, err
	}
	defer tx.Close()
	o, err = fn(ctx, tx)
	if err != nil {
		return o, err
	}
	if err := tx.Commit(ctx); err != nil {
		return o, err
	}
	return o, nil
}
