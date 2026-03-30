package registry

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	iprotodb "go.linka.cloud/protodb/internal/protodb"
	protopts "go.linka.cloud/protodb/protodb"
)

type key struct {
	field  string
	path   []protoreflect.FieldNumber
	static string
	leaf   bool
}

func (r *Registry) RegisterFile(file protoreflect.FileDescriptor) error {
	if err := r.Files.RegisterFile(file); err != nil {
		return err
	}
	r.dropFileKeys(file)
	return nil
}

func (r *Registry) RegisterMessage(mt protoreflect.MessageType) error {
	if err := r.Types.RegisterMessage(mt); err != nil {
		return err
	}
	r.dropKey(mt.Descriptor().FullName())
	return nil
}

func (r *Registry) RegisterEnum(et protoreflect.EnumType) error {
	return r.Types.RegisterEnum(et)
}

func (r *Registry) RegisterExtension(xt protoreflect.ExtensionType) error {
	return r.Types.RegisterExtension(xt)
}

func (r *Registry) KeyFieldName(md protoreflect.MessageDescriptor) (string, bool) {
	if r == nil {
		return iprotodb.KeyFieldName(md)
	}
	k, ok := r.key(md)
	if !ok {
		return "", false
	}
	if k.field != "" {
		return k.field, true
	}
	return iprotodb.KeyFieldName(md)
}

func (r *Registry) KeyFor(m proto.Message) (key, field string, err error) {
	if r == nil {
		return iprotodb.KeyFor(m)
	}
	if m == nil {
		return "", "", fmt.Errorf("key / id not found in <nil>")
	}
	k, ok := r.key(m.ProtoReflect().Descriptor())
	if !ok {
		return iprotodb.KeyFor(m)
	}
	if k.static != "" {
		return k.static, "", nil
	}
	if len(k.path) == 0 {
		return iprotodb.KeyFor(m)
	}
	field = k.field
	if !k.leaf {
		return "", field, keyNotFound(m)
	}
	v, fd, ok := keyValue(m.ProtoReflect(), k.path)
	if !ok || !v.IsValid() {
		return "", field, keyNotFound(m)
	}
	if key, ok := iprotodb.KeyString(v, fd); ok {
		return key, field, nil
	}
	return "", field, keyNotFound(m)
}

func (r *Registry) DataPrefix(m proto.Message) (key []byte, f string, value string, err error) {
	if r == nil {
		return iprotodb.DataPrefix(m)
	}
	k, f, err := r.KeyFor(m)
	if err != nil {
		return fmt.Appendf(nil, "%s/%s/", iprotodb.Data, m.ProtoReflect().Descriptor().FullName()), f, k, fmt.Errorf("key: %w", err)
	}
	return fmt.Appendf(nil, "%s/%s/%s", iprotodb.Data, m.ProtoReflect().Descriptor().FullName(), k), f, k, nil
}

func (r *Registry) key(md protoreflect.MessageDescriptor) (key, bool) {
	if r == nil {
		return key{}, false
	}
	if md == nil {
		return key{}, false
	}
	name := md.FullName()
	r.mu.RLock()
	if entry, ok := r.keys[name]; ok {
		r.mu.RUnlock()
		return entry, true
	}
	r.mu.RUnlock()
	entry := buildKeyEntry(md)
	r.mu.Lock()
	if r.keys == nil {
		r.keys = map[protoreflect.FullName]key{}
	}
	if cached, ok := r.keys[name]; ok {
		r.mu.Unlock()
		return cached, true
	}
	r.keys[name] = entry
	r.mu.Unlock()
	return entry, true
}

func (r *Registry) dropKey(name protoreflect.FullName) {
	r.mu.Lock()
	delete(r.keys, name)
	r.mu.Unlock()
}

func (r *Registry) dropFileKeys(file protoreflect.FileDescriptor) {
	r.mu.Lock()
	if len(r.keys) == 0 {
		r.mu.Unlock()
		return
	}
	msgs := file.Messages()
	for i := 0; i < msgs.Len(); i++ {
		dropNestedKeys(r.keys, msgs.Get(i))
	}
	r.mu.Unlock()
}

func dropNestedKeys(cache map[protoreflect.FullName]key, md protoreflect.MessageDescriptor) {
	delete(cache, md.FullName())
	msgs := md.Messages()
	for i := 0; i < msgs.Len(); i++ {
		dropNestedKeys(cache, msgs.Get(i))
	}
}

func buildKeyEntry(md protoreflect.MessageDescriptor) key {
	entry := key{}
	if sk, ok := staticKey(md); ok {
		entry.static = sk
		return entry
	}
	if path, ok := keyPathFromOpts(md); ok {
		entry.path = numPath(path)
		entry.field = iprotodb.KeyPathFromNames(path)
		entry.leaf = iprotodb.KeyLeaf(path[len(path)-1])
		return entry
	}
	if path, ok := inferKeyField(md); ok {
		entry.path = []protoreflect.FieldNumber{path.Number()}
		entry.field = string(path.Name())
		entry.leaf = iprotodb.KeyLeaf(path)
	}
	return entry
}

func staticKey(md protoreflect.MessageDescriptor) (string, bool) {
	v := proto.GetExtension(md.Options(), protopts.E_StaticKey)
	if v == nil {
		return "", false
	}
	sk, ok := v.(string)
	if !ok || sk == "" {
		return "", false
	}
	return sk, true
}

func inferKeyField(md protoreflect.MessageDescriptor) (protoreflect.FieldDescriptor, bool) {
	for _, name := range []protoreflect.Name{"id", "key", "name"} {
		fd := md.Fields().ByName(name)
		if fd == nil {
			continue
		}
		if !iprotodb.KeyLeaf(fd) {
			continue
		}
		return fd, true
	}
	return nil, false
}

func keyPathFromOpts(md protoreflect.MessageDescriptor) ([]protoreflect.FieldDescriptor, bool) {
	return iprotodb.KeyFieldPathFromOpts(md)
}

func numPath(path []protoreflect.FieldDescriptor) []protoreflect.FieldNumber {
	out := make([]protoreflect.FieldNumber, 0, len(path))
	for _, fd := range path {
		out = append(out, fd.Number())
	}
	return out
}

func keyValue(m protoreflect.Message, path []protoreflect.FieldNumber) (protoreflect.Value, protoreflect.FieldDescriptor, bool) {
	cur := m
	for i, num := range path {
		fd := cur.Descriptor().Fields().ByNumber(num)
		if fd == nil {
			return protoreflect.Value{}, nil, false
		}
		v := cur.Get(fd)
		if i == len(path)-1 {
			return v, fd, true
		}
		if fd.Kind() != protoreflect.MessageKind {
			return protoreflect.Value{}, nil, false
		}
		next := v.Message()
		if !next.IsValid() {
			return protoreflect.Value{}, nil, false
		}
		cur = next
	}
	return protoreflect.Value{}, nil, false
}

func keyNotFound(m proto.Message) error {
	return fmt.Errorf("key / id not found in %s", m.ProtoReflect().Type().Descriptor().FullName())
}
