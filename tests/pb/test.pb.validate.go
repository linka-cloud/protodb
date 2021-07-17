// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: tests/pb/test.proto

package pb

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/anypb"
)

// ensure the imports are used
var (
	_ = bytes.MinRead
	_ = errors.New("")
	_ = fmt.Print
	_ = utf8.UTFMax
	_ = (*regexp.Regexp)(nil)
	_ = (*strings.Reader)(nil)
	_ = net.IPv4len
	_ = time.Duration(0)
	_ = (*url.URL)(nil)
	_ = (*mail.Address)(nil)
	_ = anypb.Any{}
)

// Validate checks the field values on Interface with the rules defined in the
// proto definition for this message. If any rules are violated, an error is returned.
func (m *Interface) Validate() error {
	if m == nil {
		return nil
	}

	// no validation rules for Name

	if wrapper := m.GetMac(); wrapper != nil {

		if !_Interface_Mac_Pattern.MatchString(wrapper.GetValue()) {
			return InterfaceValidationError{
				field:  "Mac",
				reason: "value does not match regex pattern \"^([0-9A-F]{2}[:-]){5}([0-9A-F]{2})$\"",
			}
		}

	}

	if _, ok := InterfaceStatus_name[int32(m.GetStatus())]; !ok {
		return InterfaceValidationError{
			field:  "Status",
			reason: "value must be one of the defined enum values",
		}
	}

	for idx, item := range m.GetAddresses() {
		_, _ = idx, item

		if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return InterfaceValidationError{
					field:  fmt.Sprintf("Addresses[%v]", idx),
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	// no validation rules for Mtu

	return nil
}

// InterfaceValidationError is the validation error returned by
// Interface.Validate if the designated constraints aren't met.
type InterfaceValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e InterfaceValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e InterfaceValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e InterfaceValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e InterfaceValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e InterfaceValidationError) ErrorName() string { return "InterfaceValidationError" }

// Error satisfies the builtin error interface
func (e InterfaceValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sInterface.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = InterfaceValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = InterfaceValidationError{}

var _Interface_Mac_Pattern = regexp.MustCompile("^([0-9A-F]{2}[:-]){5}([0-9A-F]{2})$")

// Validate checks the field values on IPAddress with the rules defined in the
// proto definition for this message. If any rules are violated, an error is returned.
func (m *IPAddress) Validate() error {
	if m == nil {
		return nil
	}

	switch m.Address.(type) {

	case *IPAddress_IPV4:

		if ip := net.ParseIP(m.GetIPV4()); ip == nil || ip.To4() == nil {
			return IPAddressValidationError{
				field:  "Ipv4",
				reason: "value must be a valid IPv4 address",
			}
		}

	case *IPAddress_IPV6:

		if ip := net.ParseIP(m.GetIPV6()); ip == nil || ip.To4() != nil {
			return IPAddressValidationError{
				field:  "Ipv6",
				reason: "value must be a valid IPv6 address",
			}
		}

	}

	return nil
}

// IPAddressValidationError is the validation error returned by
// IPAddress.Validate if the designated constraints aren't met.
type IPAddressValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e IPAddressValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e IPAddressValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e IPAddressValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e IPAddressValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e IPAddressValidationError) ErrorName() string { return "IPAddressValidationError" }

// Error satisfies the builtin error interface
func (e IPAddressValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sIPAddress.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = IPAddressValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = IPAddressValidationError{}
