package gossip

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"go.linka.cloud/protodb/internal/badgerd/pending"
	"go.linka.cloud/protodb/internal/badgerd/replication"
	pb "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

type fakeDB struct {
	mu            sync.RWMutex
	path          string
	inMemory      bool
	maxVersion    uint64
	version       uint64
	maxBatchCount int64
	maxBatchSize  int64
	valueThr      int64
	streamFn      func(context.Context, uint64, uint64, io.Writer) error
	batch         *fakeWriteBatch
}

func (f *fakeDB) Path() string   { return f.path }
func (f *fakeDB) InMemory() bool { return f.inMemory }
func (f *fakeDB) MaxVersion() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.maxVersion
}
func (f *fakeDB) SetVersion(v uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version = v
}
func (f *fakeDB) SetMaxVersion(v uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.maxVersion = v
}
func (f *fakeDB) Drop() error                                     { return nil }
func (f *fakeDB) Load(context.Context, io.Reader) (uint64, error) { return 0, nil }
func (f *fakeDB) Stream(ctx context.Context, at, since uint64, w io.Writer) error {
	if f.streamFn != nil {
		return f.streamFn(ctx, at, since, w)
	}
	return nil
}
func (f *fakeDB) NewWriteBatchAt(uint64) replication.WriteBatch {
	if f.batch == nil {
		f.batch = &fakeWriteBatch{}
	}
	return f.batch
}
func (f *fakeDB) ValueThreshold() int64 { return f.valueThr }
func (f *fakeDB) MaxBatchCount() int64  { return f.maxBatchCount }
func (f *fakeDB) MaxBatchSize() int64   { return f.maxBatchSize }
func (f *fakeDB) Close() error          { return nil }

type batchSet struct {
	key       []byte
	value     []byte
	expiresAt uint64
	userMeta  byte
	ts        uint64
}

type batchDel struct {
	key []byte
	ts  uint64
}

type fakeWriteBatch struct {
	sets      []batchSet
	dels      []batchDel
	flushErr  error
	flushed   bool
	cancelled bool
}

func (f *fakeWriteBatch) SetEntryAt(e *badger.Entry, ts uint64) error {
	f.sets = append(f.sets, batchSet{key: append([]byte{}, e.Key...), value: append([]byte{}, e.Value...), expiresAt: e.ExpiresAt, userMeta: e.UserMeta, ts: ts})
	return nil
}

func (f *fakeWriteBatch) DeleteAt(key []byte, ts uint64) error {
	f.dels = append(f.dels, batchDel{key: append([]byte{}, key...), ts: ts})
	return nil
}

func (f *fakeWriteBatch) Flush() error {
	f.flushed = true
	return f.flushErr
}

func (f *fakeWriteBatch) Cancel() {
	f.cancelled = true
}

type fakeWrites struct {
	replayRawFn func(func([]byte, []byte, byte, uint64) error) error
	closeErr    error
}

func (f *fakeWrites) Iterator([]byte, bool) pending.Iterator { return nil }
func (f *fakeWrites) Get([]byte) (pending.Item, error)       { return nil, nil }
func (f *fakeWrites) Set(*badger.Entry)                      {}
func (f *fakeWrites) Delete([]byte)                          {}
func (f *fakeWrites) ReplayRaw(fn func([]byte, []byte, byte, uint64) error) error {
	if f.replayRawFn != nil {
		return f.replayRawFn(fn)
	}
	return nil
}
func (f *fakeWrites) Replay(func(*badger.Entry) error) error { return nil }
func (f *fakeWrites) Close() error                           { return f.closeErr }

type fakeReplicateClient struct {
	replicateErr error
	replicate    pb.ReplicationService_ReplicateClient
}

func (f *fakeReplicateClient) Init(context.Context, *pb.InitRequest, ...grpc.CallOption) (pb.ReplicationService_InitClient, error) {
	return nil, nil
}

func (f *fakeReplicateClient) Replicate(context.Context, ...grpc.CallOption) (pb.ReplicationService_ReplicateClient, error) {
	if f.replicateErr != nil {
		return nil, f.replicateErr
	}
	if f.replicate != nil {
		return f.replicate, nil
	}
	return &fakeReplicateStream{}, nil
}

func (f *fakeReplicateClient) Alive(context.Context, ...grpc.CallOption) (pb.ReplicationService_AliveClient, error) {
	return nil, nil
}

func (f *fakeReplicateClient) Election(context.Context, *pb.Message, ...grpc.CallOption) (*pb.Message, error) {
	return &pb.Message{}, nil
}

type fakeReplicateStream struct {
	sent     []*pb.ReplicateRequest
	acks     []*pb.Ack
	recvErr  error
	sendErr  error
	closeErr error
	next     int
}

func (f *fakeReplicateStream) Send(req *pb.ReplicateRequest) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, req.CloneVT())
	return nil
}

func (f *fakeReplicateStream) Recv() (*pb.Ack, error) {
	if f.next < len(f.acks) {
		v := f.acks[f.next]
		f.next++
		return v, nil
	}
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	return &pb.Ack{}, nil
}

func (f *fakeReplicateStream) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeReplicateStream) Trailer() metadata.MD         { return nil }
func (f *fakeReplicateStream) CloseSend() error             { return f.closeErr }
func (f *fakeReplicateStream) Context() context.Context     { return context.Background() }
func (f *fakeReplicateStream) SendMsg(any) error            { return nil }
func (f *fakeReplicateStream) RecvMsg(any) error            { return nil }

type fakeInitSrv struct {
	ctx  context.Context
	sent []*pb.InitResponse
}

func (f *fakeInitSrv) Send(msg *pb.InitResponse) error {
	f.sent = append(f.sent, msg)
	return nil
}

func (f *fakeInitSrv) SetHeader(metadata.MD) error  { return nil }
func (f *fakeInitSrv) SendHeader(metadata.MD) error { return nil }
func (f *fakeInitSrv) SetTrailer(metadata.MD)       {}
func (f *fakeInitSrv) Context() context.Context     { return f.ctx }
func (f *fakeInitSrv) SendMsg(any) error            { return nil }
func (f *fakeInitSrv) RecvMsg(any) error            { return nil }

type fakeReplicateSrv struct {
	ctx     context.Context
	msgs    []*pb.ReplicateRequest
	acks    int
	recvIx  int
	sendErr error
	recvErr error
}

func (f *fakeReplicateSrv) Send(*pb.Ack) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.acks++
	return nil
}

func (f *fakeReplicateSrv) Recv() (*pb.ReplicateRequest, error) {
	if f.recvIx < len(f.msgs) {
		m := f.msgs[f.recvIx]
		f.recvIx++
		return m, nil
	}
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	return nil, io.EOF
}

func (f *fakeReplicateSrv) SetHeader(metadata.MD) error  { return nil }
func (f *fakeReplicateSrv) SendHeader(metadata.MD) error { return nil }
func (f *fakeReplicateSrv) SetTrailer(metadata.MD)       {}
func (f *fakeReplicateSrv) Context() context.Context     { return f.ctx }
func (f *fakeReplicateSrv) SendMsg(any) error            { return nil }
func (f *fakeReplicateSrv) RecvMsg(any) error            { return nil }

type fakeAliveSrv struct {
	ctx      context.Context
	recvErrs []error
	sendErrs []error
	recvIx   int
	sendIx   int
}

func (f *fakeAliveSrv) Send(*pb.Ack) error {
	if f.sendIx < len(f.sendErrs) {
		err := f.sendErrs[f.sendIx]
		f.sendIx++
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeAliveSrv) Recv() (*pb.Ack, error) {
	if f.recvIx < len(f.recvErrs) {
		err := f.recvErrs[f.recvIx]
		f.recvIx++
		if err != nil {
			return nil, err
		}
		return &pb.Ack{}, nil
	}
	return nil, io.EOF
}

func (f *fakeAliveSrv) SetHeader(metadata.MD) error  { return nil }
func (f *fakeAliveSrv) SendHeader(metadata.MD) error { return nil }
func (f *fakeAliveSrv) SetTrailer(metadata.MD)       {}
func (f *fakeAliveSrv) Context() context.Context     { return f.ctx }
func (f *fakeAliveSrv) SendMsg(any) error            { return nil }
func (f *fakeAliveSrv) RecvMsg(any) error            { return nil }

func peerCtx(addr string, port int) context.Context {
	p := &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP(addr), Port: port}}
	return peer.NewContext(context.Background(), p)
}
