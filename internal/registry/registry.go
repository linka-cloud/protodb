// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package registry provides data structures to register and lookup
// protobuf descriptor types.
//
// The [Files] registry contains file descriptors and provides the ability
// to iterate over the files or lookup a specific descriptor within the files.
// [Files] only contains protobuf descriptors and has no understanding of Go
// type information that may be associated with each descriptor.
//
// The [Types] registry contains descriptor types for which there is a known
// Go type associated with that descriptor. It provides the ability to iterate
// over the registered types or lookup a type by name.
package registry

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/protobuf/reflect/protoreflect"
	preg "google.golang.org/protobuf/reflect/protoregistry"
)

// NotFound is a sentinel error value to indicate that the type was not found.
//
// Since registry lookup can happen in the critical performance path, resolvers
// must return this exact error value, not an error wrapping it.
var NotFound = errors.New("not found")

// Files is a registry for looking up or iterating over files and the
// descriptors contained within them.
// The Find and Range methods are safe for concurrent use.
type Files struct {
	// The map of descsByName contains:
	//	EnumDescriptor
	//	EnumValueDescriptor
	//	MessageDescriptor
	//	ExtensionDescriptor
	//	ServiceDescriptor
	//	*packageDescriptor
	//
	// Note that files are stored as a slice, since a package may contain
	// multiple files. Only top-level declarations are registered.
	// Note that enum values are in the top-level since that are in the same
	// scope as the parent enum.
	descsByName map[protoreflect.FullName]any
	filesByPath map[string][]protoreflect.FileDescriptor
	numFiles    int
	mu          sync.RWMutex
}

type packageDescriptor struct {
	files []protoreflect.FileDescriptor
}

// RegisterFile registers the provided file descriptor.
//
// If any descriptor within the file conflicts with the descriptor of any
// previously registered file (e.g., two enums with the same full name),
// then the file is not registered and an error is returned.
//
// It is permitted for multiple files to have the same file path.
func (r *Files) RegisterFile(file protoreflect.FileDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.descsByName == nil {
		r.descsByName = map[protoreflect.FullName]any{
			"": &packageDescriptor{},
		}
		r.filesByPath = make(map[string][]protoreflect.FileDescriptor)
	}

	conflicts := make(map[protoreflect.FileDescriptor]struct{})
	if existing := r.filesByPath[file.Path()]; len(existing) > 0 {
		for _, fd := range existing {
			conflicts[fd] = struct{}{}
		}
	}
	for name := file.Package(); name != ""; name = name.Parent() {
		if existing := r.descsByName[name]; existing != nil {
			if _, ok := existing.(*packageDescriptor); !ok {
				if d, ok := existing.(protoreflect.Descriptor); ok {
					conflicts[d.ParentFile()] = struct{}{}
				}
			}
		}
	}
	rangeTopLevelDescriptors(file, func(d protoreflect.Descriptor) {
		if existing := r.descsByName[d.FullName()]; existing != nil {
			switch existing := existing.(type) {
			case protoreflect.Descriptor:
				conflicts[existing.ParentFile()] = struct{}{}
			case *packageDescriptor:
				for _, fd := range existing.files {
					conflicts[fd] = struct{}{}
				}
			}
		}
	})
	for fd := range conflicts {
		r.removeFileLocked(fd)
	}

	for name := file.Package(); name != ""; name = name.Parent() {
		if r.descsByName[name] == nil {
			r.descsByName[name] = &packageDescriptor{}
		}
	}
	p := r.descsByName[file.Package()].(*packageDescriptor)
	p.files = append(p.files, file)
	rangeTopLevelDescriptors(file, func(d protoreflect.Descriptor) {
		r.descsByName[d.FullName()] = d
	})
	path := file.Path()
	r.filesByPath[path] = append(r.filesByPath[path], file)
	r.numFiles++
	return nil
}

func (r *Files) removeFileLocked(file protoreflect.FileDescriptor) {
	removed := false
	if r.filesByPath != nil {
		files := r.filesByPath[file.Path()]
		if len(files) > 0 {
			filtered := files[:0]
			for _, fd := range files {
				if fd == file {
					removed = true
					continue
				}
				filtered = append(filtered, fd)
			}
			if len(filtered) == 0 {
				delete(r.filesByPath, file.Path())
			} else if len(filtered) != len(files) {
				r.filesByPath[file.Path()] = filtered
			}
		}
	}
	if removed {
		r.numFiles--
	}
	if r.descsByName != nil {
		if pd, ok := r.descsByName[file.Package()].(*packageDescriptor); ok {
			if len(pd.files) > 0 {
				filtered := pd.files[:0]
				for _, fd := range pd.files {
					if fd == file {
						continue
					}
					filtered = append(filtered, fd)
				}
				pd.files = filtered
			}
		}
		rangeTopLevelDescriptors(file, func(d protoreflect.Descriptor) {
			if existing := r.descsByName[d.FullName()]; existing == d {
				delete(r.descsByName, d.FullName())
			}
		})
	}
}

// FindDescriptorByName looks up a descriptor by the full name.
//
// This returns (nil, [NotFound]) if not found.
func (r *Files) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	if r == nil {
		return nil, NotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	prefix := name
	suffix := nameSuffix("")
	for prefix != "" {
		if d, ok := r.descsByName[prefix]; ok {
			switch d := d.(type) {
			case protoreflect.EnumDescriptor:
				if d.FullName() == name {
					return d, nil
				}
			case protoreflect.EnumValueDescriptor:
				if d.FullName() == name {
					return d, nil
				}
			case protoreflect.MessageDescriptor:
				if d.FullName() == name {
					return d, nil
				}
				if d := findDescriptorInMessage(d, suffix); d != nil && d.FullName() == name {
					return d, nil
				}
			case protoreflect.ExtensionDescriptor:
				if d.FullName() == name {
					return d, nil
				}
			case protoreflect.ServiceDescriptor:
				if d.FullName() == name {
					return d, nil
				}
				if d := d.Methods().ByName(suffix.Pop()); d != nil && d.FullName() == name {
					return d, nil
				}
			}
			return nil, NotFound
		}
		prefix = prefix.Parent()
		suffix = nameSuffix(name[len(prefix)+len("."):])
	}
	return nil, NotFound
}

func findDescriptorInMessage(md protoreflect.MessageDescriptor, suffix nameSuffix) protoreflect.Descriptor {
	name := suffix.Pop()
	if suffix == "" {
		if ed := md.Enums().ByName(name); ed != nil {
			return ed
		}
		for i := md.Enums().Len() - 1; i >= 0; i-- {
			if vd := md.Enums().Get(i).Values().ByName(name); vd != nil {
				return vd
			}
		}
		if xd := md.Extensions().ByName(name); xd != nil {
			return xd
		}
		if fd := md.Fields().ByName(name); fd != nil {
			return fd
		}
		if od := md.Oneofs().ByName(name); od != nil {
			return od
		}
	}
	if md := md.Messages().ByName(name); md != nil {
		if suffix == "" {
			return md
		}
		return findDescriptorInMessage(md, suffix)
	}
	return nil
}

type nameSuffix string

func (s *nameSuffix) Pop() (name protoreflect.Name) {
	if i := strings.IndexByte(string(*s), '.'); i >= 0 {
		name, *s = protoreflect.Name((*s)[:i]), (*s)[i+1:]
	} else {
		name, *s = protoreflect.Name((*s)), ""
	}
	return name
}

// FindFileByPath looks up a file by the path.
//
// This returns (nil, [NotFound]) if not found.
// This returns an error if multiple files have the same path.
func (r *Files) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if r == nil {
		return nil, NotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	fds := r.filesByPath[path]
	switch len(fds) {
	case 0:
		return nil, NotFound
	case 1:
		return fds[0], nil
	default:
		return nil, fmt.Errorf("multiple files named %q", path)
	}
}

// NumFiles reports the number of registered files,
// including duplicate files with the same name.
func (r *Files) NumFiles() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.numFiles
}

// RangeFiles iterates over all registered files while f returns true.
// If multiple files have the same name, RangeFiles iterates over all of them.
// The iteration order is undefined.
func (r *Files) RangeFiles(f func(protoreflect.FileDescriptor) bool) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, files := range r.filesByPath {
		for _, file := range files {
			if !f(file) {
				return
			}
		}
	}
}

// NumFilesByPackage reports the number of registered files in a proto package.
func (r *Files) NumFilesByPackage(name protoreflect.FullName) int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.descsByName[name].(*packageDescriptor)
	if !ok {
		return 0
	}
	return len(p.files)
}

// RangeFilesByPackage iterates over all registered files in a given proto package
// while f returns true. The iteration order is undefined.
func (r *Files) RangeFilesByPackage(name protoreflect.FullName, f func(protoreflect.FileDescriptor) bool) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.descsByName[name].(*packageDescriptor)
	if !ok {
		return
	}
	for _, file := range p.files {
		if !f(file) {
			return
		}
	}
}

// rangeTopLevelDescriptors iterates over all top-level descriptors in a file
// which will be directly entered into the registry.
func rangeTopLevelDescriptors(fd protoreflect.FileDescriptor, f func(protoreflect.Descriptor)) {
	eds := fd.Enums()
	for i := eds.Len() - 1; i >= 0; i-- {
		f(eds.Get(i))
		vds := eds.Get(i).Values()
		for i := vds.Len() - 1; i >= 0; i-- {
			f(vds.Get(i))
		}
	}
	mds := fd.Messages()
	for i := mds.Len() - 1; i >= 0; i-- {
		f(mds.Get(i))
	}
	xds := fd.Extensions()
	for i := xds.Len() - 1; i >= 0; i-- {
		f(xds.Get(i))
	}
	sds := fd.Services()
	for i := sds.Len() - 1; i >= 0; i-- {
		f(sds.Get(i))
	}
}

var (
	_ preg.MessageTypeResolver   = (*Types)(nil)
	_ preg.ExtensionTypeResolver = (*Types)(nil)
)

// Types is a registry for looking up or iterating over descriptor types.
// The Find and Range methods are safe for concurrent use.
type Types struct {
	typesByName         typesByName
	extensionsByMessage extensionsByMessage

	numEnums      int
	numMessages   int
	numExtensions int

	mu sync.RWMutex
}

type (
	typesByName         map[protoreflect.FullName]any
	extensionsByMessage map[protoreflect.FullName]extensionsByNumber
	extensionsByNumber  map[protoreflect.FieldNumber]protoreflect.ExtensionType
)

// RegisterMessage registers the provided message type.
//
// If a naming conflict occurs, the type is not registered and an error is returned.
func (r *Types) RegisterMessage(mt protoreflect.MessageType) error {
	// Under rare circumstances getting the descriptor might recursively
	// examine the registry, so fetch it before locking.
	md := mt.Descriptor()

	r.mu.Lock()
	defer r.mu.Unlock()

	prev, existed := r.register(md, mt)
	if existed {
		switch prev := prev.(type) {
		case protoreflect.MessageType:
			// no-op
		case protoreflect.EnumType:
			r.numEnums--
			r.numMessages++
		case protoreflect.ExtensionType:
			r.numExtensions--
			r.removeExtension(prev)
			r.numMessages++
		default:
			r.numMessages++
		}
	} else {
		r.numMessages++
	}
	return nil
}

// RegisterEnum registers the provided enum type.
//
// If a naming conflict occurs, the type is not registered and an error is returned.
func (r *Types) RegisterEnum(et protoreflect.EnumType) error {
	// Under rare circumstances getting the descriptor might recursively
	// examine the registry, so fetch it before locking.
	ed := et.Descriptor()

	r.mu.Lock()
	defer r.mu.Unlock()

	prev, existed := r.register(ed, et)
	if existed {
		switch prev := prev.(type) {
		case protoreflect.EnumType:
			// no-op
		case protoreflect.MessageType:
			r.numMessages--
			r.numEnums++
		case protoreflect.ExtensionType:
			r.numExtensions--
			r.removeExtension(prev)
			r.numEnums++
		default:
			r.numEnums++
		}
	} else {
		r.numEnums++
	}
	return nil
}

// RegisterExtension registers the provided extension type.
//
// If a naming conflict occurs, the type is not registered and an error is returned.
func (r *Types) RegisterExtension(xt protoreflect.ExtensionType) error {
	// Under rare circumstances getting the descriptor might recursively
	// examine the registry, so fetch it before locking.
	//
	// A known case where this can happen: Fetching the TypeDescriptor for a
	// legacy ExtensionDesc can consult the global registry.
	xd := xt.TypeDescriptor()

	r.mu.Lock()
	defer r.mu.Unlock()

	prev, existed := r.register(xd, xt)
	if existed {
		switch prev := prev.(type) {
		case protoreflect.ExtensionType:
			r.removeExtension(prev)
		case protoreflect.MessageType:
			r.numMessages--
			r.numExtensions++
		case protoreflect.EnumType:
			r.numEnums--
			r.numExtensions++
		default:
			r.numExtensions++
		}
	}
	if r.extensionsByMessage == nil {
		r.extensionsByMessage = make(extensionsByMessage)
	}
	message := xd.ContainingMessage().FullName()
	if r.extensionsByMessage[message] == nil {
		r.extensionsByMessage[message] = make(extensionsByNumber)
	}
	r.extensionsByMessage[message][xd.Number()] = xt
	if !existed {
		r.numExtensions++
	}
	return nil
}

func (r *Types) register(desc protoreflect.Descriptor, typ any) (any, bool) {
	name := desc.FullName()
	if r.typesByName == nil {
		r.typesByName = make(typesByName)
	}
	prev, existed := r.typesByName[name]
	r.typesByName[name] = typ
	return prev, existed
}

func (r *Types) removeExtension(xt protoreflect.ExtensionType) {
	if r.extensionsByMessage == nil {
		return
	}
	xd := xt.TypeDescriptor()
	message := xd.ContainingMessage().FullName()
	byNumber := r.extensionsByMessage[message]
	if byNumber == nil {
		return
	}
	if current, ok := byNumber[xd.Number()]; ok && current == xt {
		delete(byNumber, xd.Number())
	}
	if len(byNumber) == 0 {
		delete(r.extensionsByMessage, message)
	}
}

// FindEnumByName looks up an enum by its full name.
// E.g., "google.protobuf.Field.Kind".
//
// This returns (nil, [NotFound]) if not found.
func (r *Types) FindEnumByName(enum protoreflect.FullName) (protoreflect.EnumType, error) {
	if r == nil {
		return nil, NotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if v := r.typesByName[enum]; v != nil {
		if et, _ := v.(protoreflect.EnumType); et != nil {
			return et, nil
		}
		return nil, fmt.Errorf("found wrong type: got %v, want enum", typeName(v))
	}
	return nil, NotFound
}

// FindMessageByName looks up a message by its full name,
// e.g. "google.protobuf.Any".
//
// This returns (nil, [NotFound]) if not found.
func (r *Types) FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error) {
	if r == nil {
		return nil, NotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if v := r.typesByName[message]; v != nil {
		if mt, _ := v.(protoreflect.MessageType); mt != nil {
			return mt, nil
		}
		return nil, fmt.Errorf("found wrong type: got %v, want message", typeName(v))
	}
	return nil, NotFound
}

// FindMessageByURL looks up a message by a URL identifier.
// See documentation on google.protobuf.Any.type_url for the URL format.
//
// This returns (nil, [NotFound]) if not found.
func (r *Types) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	// This function is similar to FindMessageByName but
	// truncates anything before and including '/' in the URL.
	if r == nil {
		return nil, NotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	message := protoreflect.FullName(url)
	if i := strings.LastIndexByte(url, '/'); i >= 0 {
		message = message[i+len("/"):]
	}

	if v := r.typesByName[message]; v != nil {
		if mt, _ := v.(protoreflect.MessageType); mt != nil {
			return mt, nil
		}
		return nil, fmt.Errorf("found wrong type: got %v, want message", typeName(v))
	}
	return nil, NotFound
}

// FindExtensionByName looks up a extension field by the field's full name.
// Note that this is the full name of the field as determined by
// where the extension is declared and is unrelated to the full name of the
// message being extended.
//
// This returns (nil, [NotFound]) if not found.
func (r *Types) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	if r == nil {
		return nil, NotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if v := r.typesByName[field]; v != nil {
		if xt, _ := v.(protoreflect.ExtensionType); xt != nil {
			return xt, nil
		}

		return nil, fmt.Errorf("found wrong type: got %v, want extension", typeName(v))
	}
	return nil, NotFound
}

// FindExtensionByNumber looks up a extension field by the field number
// within some parent message, identified by full name.
//
// This returns (nil, [NotFound]) if not found.
func (r *Types) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	if r == nil {
		return nil, NotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if xt, ok := r.extensionsByMessage[message][field]; ok {
		return xt, nil
	}
	return nil, NotFound
}

// NumEnums reports the number of registered enums.
func (r *Types) NumEnums() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.numEnums
}

// RangeEnums iterates over all registered enums while f returns true.
// Iteration order is undefined.
func (r *Types) RangeEnums(f func(protoreflect.EnumType) bool) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, typ := range r.typesByName {
		if et, ok := typ.(protoreflect.EnumType); ok {
			if !f(et) {
				return
			}
		}
	}
}

// NumMessages reports the number of registered messages.
func (r *Types) NumMessages() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.numMessages
}

// RangeMessages iterates over all registered messages while f returns true.
// Iteration order is undefined.
func (r *Types) RangeMessages(f func(protoreflect.MessageType) bool) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, typ := range r.typesByName {
		if mt, ok := typ.(protoreflect.MessageType); ok {
			if !f(mt) {
				return
			}
		}
	}
}

// NumExtensions reports the number of registered extensions.
func (r *Types) NumExtensions() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.numExtensions
}

// RangeExtensions iterates over all registered extensions while f returns true.
// Iteration order is undefined.
func (r *Types) RangeExtensions(f func(protoreflect.ExtensionType) bool) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, typ := range r.typesByName {
		if xt, ok := typ.(protoreflect.ExtensionType); ok {
			if !f(xt) {
				return
			}
		}
	}
}

// NumExtensionsByMessage reports the number of registered extensions for
// a given message type.
func (r *Types) NumExtensionsByMessage(message protoreflect.FullName) int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.extensionsByMessage[message])
}

// RangeExtensionsByMessage iterates over all registered extensions filtered
// by a given message type while f returns true. Iteration order is undefined.
func (r *Types) RangeExtensionsByMessage(message protoreflect.FullName, f func(protoreflect.ExtensionType) bool) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, xt := range r.extensionsByMessage[message] {
		if !f(xt) {
			return
		}
	}
}

func typeName(t any) string {
	switch t.(type) {
	case protoreflect.EnumType:
		return "enum"
	case protoreflect.MessageType:
		return "message"
	case protoreflect.ExtensionType:
		return "extension"
	default:
		return fmt.Sprintf("%T", t)
	}
}
