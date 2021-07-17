package pb

var PutRequestFields = struct {
	Payload string
}{
	Payload: "payload",
}

var PutResponseFields = struct {
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
	Put    string
	Delete string
	Commit string
}{
	Get:    "get",
	Put:    "put",
	Delete: "delete",
	Commit: "commit",
}

var TxResponseFields = struct {
	Get    string
	Put    string
	Delete string
}{
	Get:    "get",
	Put:    "put",
	Delete: "delete",
}

var PagingFields = struct {
	Limit  string
	Offset string
}{
	Limit:  "limit",
	Offset: "offset",
}

var PagingInfoFields = struct {
	HasNext string
}{
	HasNext: "has_next",
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
