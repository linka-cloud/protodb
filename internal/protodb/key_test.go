package protodb

import (
	"testing"

	"github.com/jhump/protoreflect/v2/protobuilder"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	protopts "go.linka.cloud/protodb/protodb"
)

func TestKeyForNestedKeyOption(t *testing.T) {
	md := buildNestedKeyDescriptor(t)

	m := dynamicpb.NewMessage(md)
	metaFD := md.Fields().ByName("metadata")
	meta := dynamicpb.NewMessage(metaFD.Message())
	meta.Set(metaFD.Message().Fields().ByName("id"), protoreflect.ValueOfString("r1"))
	m.Set(metaFD, protoreflect.ValueOfMessage(meta))

	key, field, err := KeyFor(m)
	require.NoError(t, err)
	require.Equal(t, "r1", key)
	require.Equal(t, "metadata.id", field)
}

func TestKeyFieldNameNestedKeyOption(t *testing.T) {
	md := buildNestedKeyDescriptor(t)
	field, ok := KeyFieldName(md)
	require.True(t, ok)
	require.Equal(t, "metadata.id", field)
}

func buildNestedKeyDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)

	meta := protobuilder.NewMessage("Metadata").
		AddField(protobuilder.NewField("id", protobuilder.FieldTypeString()).SetNumber(1).SetOptions(keyOpts)).
		AddField(protobuilder.NewField("label", protobuilder.FieldTypeString()).SetNumber(2))

	resource := protobuilder.NewMessage("Resource").
		AddField(protobuilder.NewField("metadata", protobuilder.FieldTypeMessage(meta)).SetNumber(1)).
		AddField(protobuilder.NewField("field", protobuilder.FieldTypeString()).SetNumber(2)).
		AddNestedMessage(meta)

	file := protobuilder.NewFile("tests/nested_key.proto").
		SetPackageName(protoreflect.FullName("tests.key")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(resource).
		AddImportedDependency(protopts.File_protodb_protodb_proto)

	fd, err := file.Build()
	require.NoError(t, err)
	protoFd := protodesc.ToFileDescriptorProto(fd)
	require.NotNil(t, protoFd)

	md := fd.Messages().ByName("Resource")
	require.NotNil(t, md)
	return md
}
