// Package ddbconv can be used to convert between dynamodb.AttributeValue and the Go type system
// Some of these functions provide little more than improved readability.
package ddbconv

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
		"strconv"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
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

// IntToNumber TODO: doc
func IntToNumber(i int) dynamodbattribute.Number {
	return dynamodbattribute.Number(strconv.Itoa(i))
}

// FloatToNumber TODO: doc
func FloatToNumber(f float64) dynamodbattribute.Number {
	return dynamodbattribute.Number(strconv.FormatFloat(f, 'f', -1, 64))
}

// TODO: BigIntToNumber, panics if way too big
// TODO: TryBigIntToNumber, errs if way too big
// TODO: BigFloatToNumber, panics if way too big
// TODO: TryBigFloatToNumber, errs if way too big

// FromBinary TODO: doc
func FromBinary(attr ddb.AttributeValue) []byte {
	return attr.B
}

// ToBinary TODO: doc
func ToBinary(val []byte) ddb.AttributeValue {
	return ddb.AttributeValue{B: val}
}

// FromBinarySet TODO: doc
func FromBinarySet(attr ddb.AttributeValue) [][]byte {
	return attr.BS
}

// ToBinarySet TODO: doc
func ToBinarySet(val [][]byte) ddb.AttributeValue {
	return ddb.AttributeValue{BS: val}
}

// FromInt TODO: doc
func FromInt(av ddb.AttributeValue) int {
	return requireToInt(*av.N)
}

// TryFromInt TODO: doc
func TryFromInt(av ddb.AttributeValue) (int, bool) {
	if num, ok := TryFromNumber(av); ok {
		val, err := strconv.Atoi(string(num))
		return val, err == nil
	} else {
		return 0, false
	}
}

// ToInt TODO: doc
func ToInt(val int) ddb.AttributeValue {
	return ddb.AttributeValue{N: aws.String(strconv.Itoa(val))}
}

// FromNumber TODO: doc
func FromNumber(av ddb.AttributeValue) dynamodbattribute.Number {
	return dynamodbattribute.Number(*av.N)
}

// TryFromNumber TODO: doc
func TryFromNumber(av ddb.AttributeValue) (result dynamodbattribute.Number, ok bool) {
	if av.N == nil || (av.NULL != nil && *av.NULL) {
		return "", false
	}
	return dynamodbattribute.Number(*av.N), true
}

// FromIntSet TODO: doc
func FromIntSet(attr ddb.AttributeValue) []int {
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

// ToIntSet TODO: doc
func ToIntSet(vals []int) ddb.AttributeValue {
	asStrings := make([]string, len(vals))
	for i, v := range vals {
		asStrings[i] = strconv.Itoa(v)
	}
	return ddb.AttributeValue{NS: asStrings}
}

// FromString TODO: doc
func FromString(attr ddb.AttributeValue) string {
	if attr.S == nil {
		return ""
	}
	return *attr.S
}

// ToString TODO: doc
func ToString(val string) ddb.AttributeValue {
	return ddb.AttributeValue{S: aws.String(val)}
}

// FromStringSet TODO: doc
func FromStringSet(attr ddb.AttributeValue) []string {
	return attr.SS
}

// ToStringSet TODO: doc
func ToStringSet(val []string) ddb.AttributeValue {
	return ddb.AttributeValue{SS: val}
}

// FromBool TODO: doc
func FromBool(attr ddb.AttributeValue) bool {
	return *attr.BOOL
}

// TryFromBool TODO: doc
func TryFromBool(attr ddb.AttributeValue) (val, ok bool) {
	ok = attr.BOOL != nil
	return ok && *attr.BOOL, ok
}

// ToBool TODO: doc
func ToBool(val bool) ddb.AttributeValue {
	return ddb.AttributeValue{BOOL: aws.Bool(val)}
}

// FromMap TODO: doc
func FromMap(attr ddb.AttributeValue) map[string]ddb.AttributeValue {
	return attr.M
}

// ToMap TODO: doc
func ToMap(val map[string]ddb.AttributeValue) ddb.AttributeValue {
	return ddb.AttributeValue{M: val}
}

// FromList TODO: doc
func FromList(attr ddb.AttributeValue) []ddb.AttributeValue {
	return attr.L
}

// ToList: TODO: doc
func ToList(val []ddb.AttributeValue) ddb.AttributeValue {
	return ddb.AttributeValue{L: val}
}
