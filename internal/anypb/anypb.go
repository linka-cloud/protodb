package anypb

import (
	"errors"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/known/anypb"
)

type Any = anypb.Any

func New(m proto.Message) (*anypb.Any, error) {
	a := new(anypb.Any)
	return a, MarshalFrom(a, m)
}

func MarshalFrom(dst *anypb.Any, src proto.Message) error {
	const urlPrefix = "type.googleapis.com/"
	if src == nil {
		return protoimpl.X.NewError("invalid nil source message")
	}
	b, err := marshal(src)
	if err != nil {
		return err
	}
	dst.TypeUrl = urlPrefix + string(src.ProtoReflect().Descriptor().FullName())
	dst.Value = b
	return nil
}

func marshal(msg proto.Message) ([]byte, error) {
	switch d := msg.(type) {
	case interface {
		MarshalVT() ([]byte, error)
	}:
		return d.MarshalVT()
	case interface {
		Marshal() ([]byte, error)
	}:
		return d.Marshal()
	default:
		return proto.Marshal(msg)
	}
}

func UnmarshalNew(src *anypb.Any) (dst proto.Message, err error) {
	if src.GetTypeUrl() == "" {
		return nil, protoimpl.X.NewError("invalid empty type URL")
	}
	mt, err := protoregistry.GlobalTypes.FindMessageByURL(src.GetTypeUrl())
	if err != nil {
		if errors.Is(err, protoregistry.NotFound) {
			return nil, err
		}
		return nil, protoimpl.X.NewError("could not resolve %q: %v", src.GetTypeUrl(), err)
	}
	dst = mt.New().Interface()
	return dst, unmarshal(src.GetValue(), dst)
}

func UnmarshalTo(src *anypb.Any, dst proto.Message) error {
	if src == nil {
		return protoimpl.X.NewError("invalid nil source message")
	}
	if !src.MessageIs(dst) {
		got := dst.ProtoReflect().Descriptor().FullName()
		want := src.MessageName()
		return protoimpl.X.NewError("mismatched message type: got %q, want %q", got, want)
	}
	return unmarshal(src.GetValue(), dst)
}

func unmarshal(data []byte, dst proto.Message) error {
	switch d := dst.(type) {
	case interface {
		UnmarshalVT(data []byte) error
	}:
		return d.UnmarshalVT(data)
	case interface{ Unmarshal(data []byte) error }:
		return d.Unmarshal(data)
	default:
		return proto.Unmarshal(data, dst)
	}
}
