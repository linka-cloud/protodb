package rpcerrs

import (
	"context"
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoregistry"

	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/internal/token"
)

const domain = "protodb"

type entry struct {
	err    error
	code   codes.Code
	reason string
}

var entries = []entry{
	{err: badger.ErrKeyNotFound, code: codes.NotFound, reason: "ERR_KEY_NOT_FOUND"},
	{err: badger.ErrTxnTooBig, code: codes.ResourceExhausted, reason: "ERR_TXN_TOO_BIG"},
	{err: badger.ErrConflict, code: codes.Aborted, reason: "ERR_CONFLICT"},
	{err: badger.ErrReadOnlyTxn, code: codes.FailedPrecondition, reason: "ERR_READ_ONLY_TXN"},
	{err: badger.ErrEmptyKey, code: codes.InvalidArgument, reason: "ERR_EMPTY_KEY"},
	{err: badger.ErrInvalidKey, code: codes.InvalidArgument, reason: "ERR_INVALID_KEY"},
	{err: badger.ErrBannedKey, code: codes.InvalidArgument, reason: "ERR_BANNED_KEY"},
	{err: badger.ErrInvalidRequest, code: codes.InvalidArgument, reason: "ERR_INVALID_REQUEST"},
	{err: badger.ErrInvalidDump, code: codes.FailedPrecondition, reason: "ERR_INVALID_DUMP"},
	{err: badger.ErrWindowsNotSupported, code: codes.Unimplemented, reason: "ERR_WINDOWS_NOT_SUPPORTED"},
	{err: badger.ErrPlan9NotSupported, code: codes.Unimplemented, reason: "ERR_PLAN9_NOT_SUPPORTED"},
	{err: badger.ErrTruncateNeeded, code: codes.FailedPrecondition, reason: "ERR_TRUNCATE_NEEDED"},
	{err: badger.ErrBlockedWrites, code: codes.Unavailable, reason: "ERR_BLOCKED_WRITES"},
	{err: badger.ErrEncryptionKeyMismatch, code: codes.FailedPrecondition, reason: "ERR_ENCRYPTION_KEY_MISMATCH"},
	{err: badger.ErrInvalidDataKeyID, code: codes.InvalidArgument, reason: "ERR_INVALID_DATA_KEY_ID"},
	{err: badger.ErrInvalidEncryptionKey, code: codes.InvalidArgument, reason: "ERR_INVALID_ENCRYPTION_KEY"},
	{err: badger.ErrDBClosed, code: codes.FailedPrecondition, reason: "ERR_DB_CLOSED"},
	{err: protodb.ErrNotLeader, code: codes.FailedPrecondition, reason: "ERR_NOT_LEADER"},
	{err: protodb.ErrNoLeaderConn, code: codes.Unavailable, reason: "ERR_NO_LEADER_CONN"},
	{err: token.ErrInvalid, code: codes.InvalidArgument, reason: "ERR_INVALID_CONTINUATION_TOKEN"},
	{err: protoregistry.NotFound, code: codes.NotFound, reason: "ERR_TYPE_NOT_FOUND"},
}

var reasonErr = map[string]error{}

func init() {
	for _, v := range entries {
		reasonErr[v.reason] = v.err
	}
}

func ToStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, context.Canceled.Error())
	}
	for _, v := range entries {
		if !errors.Is(err, v.err) {
			continue
		}
		s := status.New(v.code, v.err.Error())
		sd, e := s.WithDetails(&errdetails.ErrorInfo{Reason: v.reason, Domain: domain})
		if e != nil {
			return s.Err()
		}
		return sd.Err()
	}
	return err
}

func FromStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	s, ok := status.FromError(err)
	if !ok {
		return err
	}
	if s.Code() == codes.Canceled {
		return context.Canceled
	}
	for _, d := range s.Details() {
		ei, ok := d.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		e, ok := reasonErr[ei.Reason]
		if ok {
			return e
		}
	}
	for _, v := range entries {
		if s.Message() == v.err.Error() || strings.Contains(s.Message(), v.err.Error()) {
			return v.err
		}
	}
	return err
}

func FromMessage(msg string) error {
	for _, v := range entries {
		if msg == v.err.Error() || strings.Contains(msg, v.err.Error()) {
			return v.err
		}
	}
	return errors.New(msg)
}
