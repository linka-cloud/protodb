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
	Search  string
	Filters string
	Paging  string
}{
	Search:  "search",
	Filters: "filters",
	Paging:  "paging",
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
}{
	Get:    "get",
	Set:    "set",
	Delete: "delete",
	Commit: "commit",
}

var TxResponseFields = struct {
	Get    string
	Set    string
	Delete string
}{
	Get:    "get",
	Set:    "set",
	Delete: "delete",
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
	Search  string
	Filters string
}{
	Search:  "search",
	Filters: "filters",
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
