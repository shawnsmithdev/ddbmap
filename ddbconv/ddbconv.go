// Package ddbconv can be used to convert between dynamodb.AttributeValue and the Go type system
// Some of these functions provide little more than improved readability.
package ddbconv

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
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
func IntToNumber(i int) dynamodbattribute.Number {
	return dynamodbattribute.Number(strconv.Itoa(i))
}

// FloatToNumber converts a float64 into a Number.
func FloatToNumber(f float64) dynamodbattribute.Number {
	return dynamodbattribute.Number(strconv.FormatFloat(f, 'f', -1, 64))
}

// EncodeNumber converts a Number into an AttributeValue with the Number (N) type.
func EncodeNumber(n dynamodbattribute.Number) dynamodb.AttributeValue {
	str := n.String()
	return dynamodb.AttributeValue{N: &str}
}

// DecodeBinary converts an AttributeValue into a []byte,
// which will be empty if the value is not a Binary (B).
func DecodeBinary(attr dynamodb.AttributeValue) []byte {
	return attr.B
}

// EncodeBinary converts a []byte into an AttributeValue with the Binary (B) type.
func EncodeBinary(val []byte) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{B: val}
}

// DecodeBinarySet converts an AttributeValue into a [][]byte,
// which will be empty if the value is not a BinarySet (BS).
func DecodeBinarySet(attr dynamodb.AttributeValue) [][]byte {
	return attr.BS
}

// EncodeBinarySet converts a [][]byte into an AttributeValue with the BinarySet (BS) type.
func EncodeBinarySet(val [][]byte) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{BS: val}
}

// DecodeInt converts an AttributeValue into an int, and will panic if the value is not an integral Number,
// if it is a NULL, or if it will not fit in an int without losing precision.
func DecodeInt(av dynamodb.AttributeValue) int {
	return requireToInt(*av.N)
}

// TryDecodeInt attempts to convert an AttributeValue into an int.
// The boolean result is true if the decode was successful.
func TryDecodeInt(av dynamodb.AttributeValue) (int, bool) {
	if num, ok := TryDecodeNumber(av); ok {
		val, err := strconv.Atoi(num.String())
		return val, err == nil
	}
	return 0, false
}

// EncodeInt converts an int into an AttributeValue with the Number (N) type.
func EncodeInt(val int) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{N: aws.String(strconv.Itoa(val))}
}

// DecodeNumber converts an AttributeValue into a Number, and will panic if the value is not a Number (N),
// or if the value is a NULL.
func DecodeNumber(av dynamodb.AttributeValue) dynamodbattribute.Number {
	return dynamodbattribute.Number(*av.N)
}

// TryDecodeNumber attempts to convert and AttributeValue into a Number.
// The boolean result is true if the value is a Number (N).
func TryDecodeNumber(av dynamodb.AttributeValue) (result dynamodbattribute.Number, ok bool) {
	ok = av.N != nil && !IsNull(av)
	if ok {
		result = dynamodbattribute.Number(*av.N)
	}
	return result, ok
}

// DecodeIntSet converts an AttributeValue into an []int, which will be empty if the value is not a NumberSet (NS),
// or if any value in the set is not an integral number that will fit in an int.
func DecodeIntSet(attr dynamodb.AttributeValue) []int {
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
func EncodeIntSet(vals []int) dynamodb.AttributeValue {
	asStrings := make([]string, len(vals))
	for i, v := range vals {
		asStrings[i] = strconv.Itoa(v)
	}
	return dynamodb.AttributeValue{NS: asStrings}
}

// DecodeString converts an AttributeValue into a String,
// which will be empty if the value if not a String (S).
func DecodeString(attr dynamodb.AttributeValue) string {
	result, _ := TryDecodeString(attr)
	return result
}

// TryDecodeString attempts to convert an AttributeValue into a string.
// The ok result is true if the value is a String (S).
func TryDecodeString(attr dynamodb.AttributeValue) (result string, ok bool) {
	ok = attr.S != nil && !IsNull(attr)
	if ok {
		result = *attr.S
	}
	return result, ok
}

// EncodeString converts a string into an AttributeValue with the String (S) type.
func EncodeString(val string) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{S: aws.String(val)}
}

// DecodeStringSet converts an AttributeValue into a []string,
// which will be empty if the value is not a StringSet (SS).
func DecodeStringSet(attr dynamodb.AttributeValue) []string {
	return attr.SS
}

// EncodeStringSet converts a []string into an AttributeValue with the StringSet (SS) type.
func EncodeStringSet(val []string) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{SS: val}
}

// DecodeBool converts an AttributeValue into a bool,
// and will panic if the value is not a Boolean (BOOL).
func DecodeBool(attr dynamodb.AttributeValue) bool {
	return *attr.BOOL
}

// TryDecodeBool attempts to convert an AttributeValue into a bool.
// The ok result is true if the value is a Boolean (BOOL).
func TryDecodeBool(attr dynamodb.AttributeValue) (val, ok bool) {
	ok = attr.BOOL != nil && !IsNull(attr)
	return ok && *attr.BOOL, ok
}

// EncodeBool converts a bool into an AttributeValue with the Boolean (BOOL) type.
func EncodeBool(val bool) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{BOOL: aws.Bool(val)}
}

// DecodeMap converts an AttributeValue into a map[string]AttributeValue,
// which will be empty if the value is not a Map (M).
func DecodeMap(attr dynamodb.AttributeValue) map[string]dynamodb.AttributeValue {
	return attr.M
}

// EncodeMap converts a map[string]AttributeValue into an AttributeValue with the Map (M) type.
func EncodeMap(val map[string]dynamodb.AttributeValue) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{M: val}
}

// DecodeList converts an AttributeValue into a []AttributeValue,
// which will be empty if the value is not a List (L).
func DecodeList(attr dynamodb.AttributeValue) []dynamodb.AttributeValue {
	return attr.L
}

// EncodeList converts a []AttributeValue into an AttributeValue with the List (L) type.
func EncodeList(val []dynamodb.AttributeValue) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{L: val}
}

// IsNull returns true if the given AttributeValue is a Null (NULL).
func IsNull(attr dynamodb.AttributeValue) bool {
	return attr.NULL != nil && *attr.NULL
}
