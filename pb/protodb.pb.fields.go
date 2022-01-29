package pb

var SetRequestFields = struct {
	Payload string
	Ttl     string
}{
	Payload: "payload",
	Ttl:     "ttl",
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
	Search string
	Filter string
	Paging string
}{
	Search: "search",
	Filter: "filter",
	Paging: "paging",
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
