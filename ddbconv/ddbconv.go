// Package ddbconv can be used to convert between dynamodb.AttributeValue and the Go type system
// Some of these functions provide little more than improved readability.
package ddbconv

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
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

func FromBinary(attr ddb.AttributeValue) []byte {
	return attr.B
}
func ToBinary(val []byte) ddb.AttributeValue {
	return ddb.AttributeValue{B: val}
}
func FromBinarySet(attr ddb.AttributeValue) [][]byte {
	return attr.BS
}
func ToBinarySet(val [][]byte) ddb.AttributeValue {
	return ddb.AttributeValue{BS: val}
}
func FromNumber(av ddb.AttributeValue) int {
	return requireToInt(*av.N)
}
func TryFromNumber(av ddb.AttributeValue) (int, bool) {
	if av.N == nil || (av.NULL != nil && *av.NULL) {
		return 0, false
	}
	val, err := strconv.Atoi(*av.N)
	return val, err == nil
}

func ToNumber(val int) ddb.AttributeValue {
	return ddb.AttributeValue{N: aws.String(strconv.Itoa(val))}
}
func FromNumberSet(attr ddb.AttributeValue) []int {
	asStrings := attr.NS
	val := make([]int, len(asStrings))
	for i, s := range asStrings {
		val[i] = requireToInt(s)
	}
	return val
}
func ToNumberSet(val []int) ddb.AttributeValue {
	asStrings := make([]string, len(val))
	for i, v := range val {
		asStrings[i] = strconv.Itoa(v)
	}
	return ddb.AttributeValue{NS: asStrings}
}
func FromString(attr ddb.AttributeValue) string {
	return *attr.S
}
func ToString(val string) ddb.AttributeValue {
	return ddb.AttributeValue{S: aws.String(val)}
}
func FromStringSet(attr ddb.AttributeValue) []string {
	return attr.SS
}
func ToStringSet(val []string) ddb.AttributeValue {
	return ddb.AttributeValue{SS: val}
}
func FromBool(attr ddb.AttributeValue) bool {
	return *attr.BOOL
}
func ToBool(val bool) ddb.AttributeValue {
	return ddb.AttributeValue{BOOL: aws.Bool(val)}
}
func FromMap(attr ddb.AttributeValue) map[string]ddb.AttributeValue {
	return attr.M
}
func ToMap(val map[string]ddb.AttributeValue) ddb.AttributeValue {
	return ddb.AttributeValue{M: val}
}
func FromList(attr ddb.AttributeValue) []ddb.AttributeValue {
	return attr.L
}
func ToList(val []ddb.AttributeValue) ddb.AttributeValue {
	return ddb.AttributeValue{L: val}
}
