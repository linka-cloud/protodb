package registry

import (
	"testing"

	"github.com/jhump/protoreflect/v2/protobuilder"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	protopts "go.linka.cloud/protodb/protodb"
)

func TestRegistryKeyCacheLazyAndInvalidate(t *testing.T) {
	r := &Registry{Files: &Files{}, Types: &Types{}}

	fd1 := buildResourceFile(t, "acme/v1/resource.proto", true)
	require.NoError(t, r.RegisterFile(fd1))
	require.Empty(t, r.keys)

	d1, err := r.FindDescriptorByName("acme.v1.Resource")
	require.NoError(t, err)
	md1 := d1.(protoreflect.MessageDescriptor)

	field, ok := r.KeyFieldName(md1)
	require.True(t, ok)
	require.Equal(t, "metadata.id", field)
	require.Len(t, r.keys, 1)

	m1 := dynamicpb.NewMessage(md1)
	metaFD1 := md1.Fields().ByName("metadata")
	meta1 := dynamicpb.NewMessage(metaFD1.Message())
	meta1.Set(metaFD1.Message().Fields().ByName("id"), protoreflect.ValueOfString("r1"))
	m1.Set(metaFD1, protoreflect.ValueOfMessage(meta1))

	key, field, err := r.KeyFor(m1)
	require.NoError(t, err)
	require.Equal(t, "r1", key)
	require.Equal(t, "metadata.id", field)

	fd2 := buildResourceFile(t, "acme/v1/resource.proto", false)
	require.NoError(t, r.RegisterFile(fd2))
	require.Empty(t, r.keys)

	d2, err := r.FindDescriptorByName("acme.v1.Resource")
	require.NoError(t, err)
	md2 := d2.(protoreflect.MessageDescriptor)

	field, ok = r.KeyFieldName(md2)
	require.True(t, ok)
	require.Equal(t, "name", field)
	require.Len(t, r.keys, 1)

	m2 := dynamicpb.NewMessage(md2)
	m2.Set(md2.Fields().ByName("name"), protoreflect.ValueOfString("r2"))

	key, field, err = r.KeyFor(m2)
	require.NoError(t, err)
	require.Equal(t, "r2", key)
	require.Equal(t, "name", field)
}

func TestRegistryKeyFallbackPrefersIDOverName(t *testing.T) {
	r := &Registry{Files: &Files{}, Types: &Types{}}

	fd := buildFallbackOrderFile(t)
	require.NoError(t, r.RegisterFile(fd))

	d, err := r.FindDescriptorByName("acme.v1.NamedResource")
	require.NoError(t, err)
	md := d.(protoreflect.MessageDescriptor)

	field, ok := r.KeyFieldName(md)
	require.True(t, ok)
	require.Equal(t, "id", field)

	m := dynamicpb.NewMessage(md)
	m.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("n1"))
	m.Set(md.Fields().ByName("id"), protoreflect.ValueOfString("i1"))

	key, field, err := r.KeyFor(m)
	require.NoError(t, err)
	require.Equal(t, "i1", key)
	require.Equal(t, "id", field)
}

func buildResourceFile(t *testing.T, path string, nested bool) protoreflect.FileDescriptor {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)

	id := protobuilder.NewField("id", protobuilder.FieldTypeString()).SetNumber(1)
	if nested {
		id = id.SetOptions(keyOpts)
	}
	meta := protobuilder.NewMessage("Metadata").
		AddField(id)

	name := protobuilder.NewField("name", protobuilder.FieldTypeString()).SetNumber(2)
	if !nested {
		name = name.SetOptions(keyOpts)
	}

	resource := protobuilder.NewMessage("Resource").
		AddField(protobuilder.NewField("metadata", protobuilder.FieldTypeMessage(meta)).SetNumber(1)).
		AddField(name).
		AddNestedMessage(meta)

	file := protobuilder.NewFile(path).
		SetPackageName(protoreflect.FullName("acme.v1")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(resource).
		AddImportedDependency(protopts.File_protodb_protodb_proto)

	fd, err := file.Build()
	require.NoError(t, err)
	return fd
}

func buildFallbackOrderFile(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	msg := protobuilder.NewMessage("NamedResource").
		AddField(protobuilder.NewField("name", protobuilder.FieldTypeString()).SetNumber(1)).
		AddField(protobuilder.NewField("id", protobuilder.FieldTypeString()).SetNumber(2))

	file := protobuilder.NewFile("acme/v1/named_resource.proto").
		SetPackageName(protoreflect.FullName("acme.v1")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg)

	fd, err := file.Build()
	require.NoError(t, err)
	return fd
}
