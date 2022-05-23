package testpb

var MessageWithKeyOptionFields = struct {
	KeyField string
}{
	KeyField: "key_field",
}

var MessageWithStaticKeyFields = struct {
	Name string
}{
	Name: "name",
}

var InterfaceFields = struct {
	Name      string
	Mac       string
	Status    string
	Addresses string
	Mtu       string
}{
	Name:      "name",
	Mac:       "mac",
	Status:    "status",
	Addresses: "addresses",
	Mtu:       "mtu",
}

var IPAddressFields = struct {
	Ipv4 string
	Ipv6 string
}{
	Ipv4: "ipv4",
	Ipv6: "ipv6",
}
