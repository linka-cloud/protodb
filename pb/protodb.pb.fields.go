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

// Code generated by protoc-gen-defaults. DO NOT EDIT.

package pb

var ProtoDBMethods = struct {
	Get             string
	Set             string
	Delete          string
	Tx              string
	Watch           string
	Register        string
	Descriptors     string
	FileDescriptors string
}{
	Get:             "/linka.cloud.protodb.ProtoDB/Get",
	Set:             "/linka.cloud.protodb.ProtoDB/Set",
	Delete:          "/linka.cloud.protodb.ProtoDB/Delete",
	Tx:              "/linka.cloud.protodb.ProtoDB/Tx",
	Watch:           "/linka.cloud.protodb.ProtoDB/Watch",
	Register:        "/linka.cloud.protodb.ProtoDB/Register",
	Descriptors:     "/linka.cloud.protodb.ProtoDB/Descriptors",
	FileDescriptors: "/linka.cloud.protodb.ProtoDB/FileDescriptors",
}

var SetRequestFields = struct {
	Payload   string
	Ttl       string
	FieldMask string
}{
	Payload:   "payload",
	Ttl:       "ttl",
	FieldMask: "field_mask",
}

var SetResponseFields = struct {
	Result string
}{
	Result: "result",
}

var DeleteRequestFields = struct {
	Payload string
}{
	Payload: "payload",
}

var DeleteResponseFields = struct {
}{}

var GetRequestFields = struct {
	Search    string
	Filter    string
	Paging    string
	FieldMask string
}{
	Search:    "search",
	Filter:    "filter",
	Paging:    "paging",
	FieldMask: "field_mask",
}

var GetResponseFields = struct {
	Results string
	Paging  string
}{
	Results: "results",
	Paging:  "paging",
}

var TxRequestFields = struct {
	Get    string
	Set    string
	Delete string
	Commit string
	Count  string
	Size   string
}{
	Get:    "get",
	Set:    "set",
	Delete: "delete",
	Commit: "commit",
	Count:  "count",
	Size:   "size",
}

var TxResponseFields = struct {
	Get    string
	Set    string
	Delete string
	Commit string
	Count  string
	Size   string
}{
	Get:    "get",
	Set:    "set",
	Delete: "delete",
	Commit: "commit",
	Count:  "count",
	Size:   "size",
}

var CommitResponseFields = struct {
	Error string
}{
	Error: "error",
}

var CountRequestFields = struct {
}{}

var CountResponseFields = struct {
	Count string
}{
	Count: "count",
}

var SizeRequestFields = struct {
}{}

var SizeResponseFields = struct {
	Size string
}{
	Size: "size",
}

var PagingFields = struct {
	Limit  string
	Offset string
	Token  string
}{
	Limit:  "limit",
	Offset: "offset",
	Token:  "token",
}

var PagingInfoFields = struct {
	HasNext string
	Token   string
}{
	HasNext: "has_next",
	Token:   "token",
}

var WatchRequestFields = struct {
	Search string
	Filter string
}{
	Search: "search",
	Filter: "filter",
}

var WatchEventFields = struct {
	Type string
	Old  string
	New  string
}{
	Type: "type",
	Old:  "old",
	New:  "new",
}

var RegisterRequestFields = struct {
	File string
}{
	File: "file",
}

var RegisterResponseFields = struct {
}{}

var DescriptorsRequestFields = struct {
}{}

var DescriptorsResponseFields = struct {
	Results string
}{
	Results: "results",
}

var FileDescriptorsRequestFields = struct {
}{}

var FileDescriptorsResponseFields = struct {
	Results string
}{
	Results: "results",
}

var MessageDiffFields = struct {
	Fields string
}{
	Fields: "fields",
}

var FieldDiffFields = struct {
	From string
	To   string
}{
	From: "from",
	To:   "to",
}
