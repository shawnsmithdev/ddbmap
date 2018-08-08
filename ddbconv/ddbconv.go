// Package ddbconv can be used to convert between dynamodb.AttributeValue and the Go type system
// Some of these functions provide little more than improved readability.
package ddbconv

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbattr "github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"strconv"
)

func forbidErr(err error) {
	if err != nil {
		panic(err)
	}
}

func requireToInt(s string) int {
	val, err := strconv.Atoi(s)
	forbidErr(err)
	return val
}

// IntToNumber converts an int into a Number.
func IntToNumber(i int) ddbattr.Number {
	return ddbattr.Number(strconv.Itoa(i))
}

// FloatToNumber converts a float64 into a Number.
func FloatToNumber(f float64) ddbattr.Number {
	return ddbattr.Number(strconv.FormatFloat(f, 'f', -1, 64))
}

// EncodeNumber converts a Number into an AttributeValue with the Number (N) type.
func EncodeNumber(n ddbattr.Number) ddb.AttributeValue {
	str := n.String()
	return ddb.AttributeValue{N: &str}
}

// DecodeBinary converts an AttributeValue into a []byte,
// which will be empty if the value is not a Binary (B).
func DecodeBinary(attr ddb.AttributeValue) []byte {
	return attr.B
}

// EncodeBinary converts a []byte into an AttributeValue with the Binary (B) type.
func EncodeBinary(val []byte) ddb.AttributeValue {
	return ddb.AttributeValue{B: val}
}

// DecodeBinarySet converts an AttributeValue into a [][]byte,
// which will be empty if the value is not a BinarySet (BS).
func DecodeBinarySet(attr ddb.AttributeValue) [][]byte {
	return attr.BS
}

// EncodeBinarySet converts a [][]byte into an AttributeValue with the BinarySet (BS) type.
func EncodeBinarySet(val [][]byte) ddb.AttributeValue {
	return ddb.AttributeValue{BS: val}
}

// DecodeInt converts an AttributeValue into an int, and will panic if the value is not an integral Number,
// if it is a NULL, or if it will not fit in an int without losing precision.
func DecodeInt(av ddb.AttributeValue) int {
	return requireToInt(*av.N)
}

// TryDecodeInt attempts to convert an AttributeValue into an int.
// The boolean result is true if the decode was successful.
func TryDecodeInt(av ddb.AttributeValue) (int, bool) {
	if num, ok := TryDecodeNumber(av); ok {
		val, err := strconv.Atoi(num.String())
		return val, err == nil
	}
	return 0, false
}

// EncodeInt converts an int into an AttributeValue with the Number (N) type.
func EncodeInt(val int) ddb.AttributeValue {
	return ddb.AttributeValue{N: aws.String(strconv.Itoa(val))}
}

// DecodeNumber converts an AttributeValue into a Number, and will panic if the value is not a Number (N),
// or if the value is a NULL.
func DecodeNumber(av ddb.AttributeValue) ddbattr.Number {
	return ddbattr.Number(*av.N)
}

// TryDecodeNumber attempts to convert and AttributeValue into a Number.
// The boolean result is true if the value is a Number (N).
func TryDecodeNumber(av ddb.AttributeValue) (result ddbattr.Number, ok bool) {
	ok = av.N != nil && !IsNull(av)
	if ok {
		result = ddbattr.Number(*av.N)
	}
	return result, ok
}

// DecodeIntSet converts an AttributeValue into an []int, which will be empty if the value is not a NumberSet (NS),
// or if any value in the set is not an integral number that will fit in an int.
func DecodeIntSet(attr ddb.AttributeValue) []int {
	asStrings := attr.NS
	val := make([]int, len(asStrings))
	var err error
	for i, s := range asStrings {
		val[i], err = strconv.Atoi(s)
		if err != nil { // not an int, give up
			return []int{}
		}
	}
	return val
}

// EncodeIntSet converts an []int into an AttributeValue with the NumberSet (NS) type.
func EncodeIntSet(vals []int) ddb.AttributeValue {
	asStrings := make([]string, len(vals))
	for i, v := range vals {
		asStrings[i] = strconv.Itoa(v)
	}
	return ddb.AttributeValue{NS: asStrings}
}

// DecodeString converts an AttributeValue into a String,
// which will be empty if the value if not a String (S).
func DecodeString(attr ddb.AttributeValue) string {
	result, _ := TryDecodeString(attr)
	return result
}

// TryDecodeString attempts to convert an AttributeValue into a string.
// The ok result is true if the value is a String (S).
func TryDecodeString(attr ddb.AttributeValue) (result string, ok bool) {
	ok = attr.S != nil && !IsNull(attr)
	if ok {
		result = *attr.S
	}
	return result, ok
}

// EncodeString converts a string into an AttributeValue with the String (S) type.
func EncodeString(val string) ddb.AttributeValue {
	return ddb.AttributeValue{S: aws.String(val)}
}

// DecodeStringSet converts an AttributeValue into a []string,
// which will be empty if the value is not a StringSet (SS).
func DecodeStringSet(attr ddb.AttributeValue) []string {
	return attr.SS
}

// EncodeStringSet converts a []string into an AttributeValue with the StringSet (SS) type.
func EncodeStringSet(val []string) ddb.AttributeValue {
	return ddb.AttributeValue{SS: val}
}

// DecodeBool converts an AttributeValue into a bool,
// and will panic if the value is not a Boolean (BOOL).
func DecodeBool(attr ddb.AttributeValue) bool {
	return *attr.BOOL
}

// TryDecodeBool attempts to convert an AttributeValue into a bool.
// The ok result is true if the value is a Boolean (BOOL).
func TryDecodeBool(attr ddb.AttributeValue) (val, ok bool) {
	ok = attr.BOOL != nil && !IsNull(attr)
	return ok && *attr.BOOL, ok
}

// EncodeBool converts a bool into an AttributeValue with the Boolean (BOOL) type.
func EncodeBool(val bool) ddb.AttributeValue {
	return ddb.AttributeValue{BOOL: aws.Bool(val)}
}

// DecodeMap converts an AttributeValue into a map[string]AttributeValue,
// which will be empty if the value is not a Map (M).
func DecodeMap(attr ddb.AttributeValue) map[string]ddb.AttributeValue {
	return attr.M
}

// EncodeMap converts a map[string]AttributeValue into an AttributeValue with the Map (M) type.
func EncodeMap(val map[string]ddb.AttributeValue) ddb.AttributeValue {
	return ddb.AttributeValue{M: val}
}

// DecodeList converts an AttributeValue into a []AttributeValue,
// which will be empty if the value is not a List (L).
func DecodeList(attr ddb.AttributeValue) []ddb.AttributeValue {
	return attr.L
}

// EncodeList converts a []AttributeValue into an AttributeValue with the List (L) type.
func EncodeList(val []ddb.AttributeValue) ddb.AttributeValue {
	return ddb.AttributeValue{L: val}
}

// IsNull returns true if the given AttributeValue is a Null (NULL).
func IsNull(attr ddb.AttributeValue) bool {
	return attr.NULL != nil && *attr.NULL
}
