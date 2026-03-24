package token

import "testing"

func FuzzTokenDecodeNoPanic(f *testing.F) {
	f.Add("")
	f.Add("aGVsbG8=")
	f.Add("%%%")

	f.Fuzz(func(t *testing.T, in string) {
		var tk Token
		_ = tk.Decode(in)
	})
}

func FuzzTokenRoundTrip(f *testing.F) {
	f.Add("type", "filters", false, "order")
	f.Add("", "", true, "")

	f.Fuzz(func(t *testing.T, typ, filters string, reverse bool, order string) {
		orig := &Token{Type: typ, FiltersHash: filters, Reverse: reverse, OrderHash: order}
		s, err := orig.Encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out Token
		if err := out.Decode(s); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if err := out.ValidateFor(orig); err != nil {
			t.Fatalf("validate: %v", err)
		}
	})
}
