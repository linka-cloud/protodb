package gossip

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"go.linka.cloud/protodb/internal/badgerd/replication"
	pb "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

func TestWriterWriteChunksAtMaxMsgSize(t *testing.T) {
	ss := &fakeInitServer{}
	w := &writer{ss: ss}

	in := make([]byte, replication.MaxMsgSize*2+123)
	n, err := w.Write(in)
	require.NoError(t, err)
	assert.Equal(t, len(in), n)
	require.Len(t, ss.sent, 3)
	assert.Len(t, ss.sent[0].Data, replication.MaxMsgSize)
	assert.Len(t, ss.sent[1].Data, replication.MaxMsgSize)
	assert.Len(t, ss.sent[2].Data, 123)
}

func TestWriterWriteSendError(t *testing.T) {
	ss := &fakeInitServer{sendErr: errors.New("send")}
	w := &writer{ss: ss}

	n, err := w.Write([]byte("abc"))
	require.EqualError(t, err, "send")
	assert.Equal(t, 0, n)
}

func TestReaderReadPartialBufferTable(t *testing.T) {
	ss := &fakeInitClient{msgs: []*pb.InitResponse{{Data: []byte("abcdefgh")}}}
	r := &reader{ss: ss}

	p := make([]byte, 3)
	n, err := r.Read(p)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("abc"), p)

	n, err = r.Read(p)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("def"), p)

	n, err = r.Read(p)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []byte("gh"), p[:n])
}

func TestReaderReadRecvError(t *testing.T) {
	r := &reader{ss: &fakeInitClient{recvErr: io.EOF}}
	n, err := r.Read(make([]byte, 8))
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, n)
}

type fakeInitServer struct {
	sent    []*pb.InitResponse
	sendErr error
}

func (f *fakeInitServer) Send(msg *pb.InitResponse) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, msg)
	return nil
}

func (f *fakeInitServer) SetHeader(metadata.MD) error  { return nil }
func (f *fakeInitServer) SendHeader(metadata.MD) error { return nil }
func (f *fakeInitServer) SetTrailer(metadata.MD)       {}
func (f *fakeInitServer) Context() context.Context     { return context.Background() }
func (f *fakeInitServer) SendMsg(any) error            { return nil }
func (f *fakeInitServer) RecvMsg(any) error            { return nil }

type fakeInitClient struct {
	msgs    []*pb.InitResponse
	next    int
	recvErr error
}

func (f *fakeInitClient) Recv() (*pb.InitResponse, error) {
	if f.next < len(f.msgs) {
		m := f.msgs[f.next]
		f.next++
		return m, nil
	}
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	return nil, io.EOF
}

func (f *fakeInitClient) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeInitClient) Trailer() metadata.MD         { return nil }
func (f *fakeInitClient) CloseSend() error             { return nil }
func (f *fakeInitClient) Context() context.Context     { return context.Background() }
func (f *fakeInitClient) SendMsg(any) error            { return nil }
func (f *fakeInitClient) RecvMsg(any) error            { return nil }
