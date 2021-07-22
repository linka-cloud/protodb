package token

var TokenFields = struct {
	Ts          string
	Type        string
	LastPrefix  string
	FiltersHash string
}{
	Ts:          "ts",
	Type:        "type",
	LastPrefix:  "last_prefix",
	FiltersHash: "filters_hash",
}
