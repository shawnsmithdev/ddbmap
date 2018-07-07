package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"strconv"
)

// Item is a type alias for the output of dynamodbattribute.MarshalMap.
// This represents a bridge between the Go and DynamoDB type systems.
type Item map[string]dynamodb.AttributeValue

func requireToInt(s string) int {
	val, err := strconv.Atoi(s)
	forbidErr(err)
	return val
}
func requireToInt64(s string) int64 {
	val, err := strconv.ParseInt(s, 10, 64)
	forbidErr(err)
	return val
}

func NToInt(av dynamodb.AttributeValue) int {
	return requireToInt(*av.N)
}
func IntToN(val int) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{N: aws.String(strconv.Itoa(val))}
}
func NToInt64(av dynamodb.AttributeValue) int64 {
	return requireToInt64(*av.N)
}
func Int64ToN(val int64) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(val, 10))}
}
func StringToS(val string) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{S: aws.String(val)}
}
func BoolToBOOL(val bool) dynamodb.AttributeValue {
	return dynamodb.AttributeValue{BOOL: aws.Bool(val)}
}

func (item Item) GetBinary(attr string) []byte {
	return item[attr].B
}
func (item Item) GetBool(attr string) bool {
	return *item[attr].BOOL
}
func (item Item) GetString(attr string) string {
	return *item[attr].S
}
func (item Item) GetBinarySet(attr string) [][]byte {
	return item[attr].BS
}
func (item Item) GetList(attr string) []dynamodb.AttributeValue {
	return item[attr].L // TODO: better
}
func (item Item) GetItem(attr string) Item {
	return item[attr].M
}
func (item Item) GetInt(attr string) int {
	return NToInt(item[attr])
}
func (item Item) GetInt64(attr string) int64 {
	return NToInt64(item[attr])
}
func (item Item) GetInts(attr string) []int {
	asStrings := item[attr].NS
	val := make([]int, len(asStrings))
	for i, s := range asStrings {
		val[i] = requireToInt(s)
	}
	return val
}
func (item Item) GetStrings(attr string) []string {
	return item[attr].SS
}
func (item Item) IsNull(attr string) bool {
	return *item[attr].NULL
}

// Itemable is implemented by types that can directly build representations of their data in the DynamoDB type system.
// This allows users to avoid attribute tags and reflection.
type Itemable interface {
	AsItem() Item
}

type Keyable interface {
	Key() interface{}
}

// ItemMap is a Map that supports Itemable types as well
// Note that for DynamoDB a key must at least contain the hash key, and, if the table so configured, the range key.
type ItemMap interface {
	Map

	// DeleteItem deletes the item for the given key, if it exists.
	DeleteItem(key Itemable)

	// LoadItem returns the item stored under the given key, if present.
	// The ok result indicates whether the value was found.
	LoadItem(key Itemable) (item Item, ok bool)

	// StoreItem stores the item in the table
	StoreItem(item Itemable)

	// LoadOrStoreItem returns the item stored under the given key, if present.
	// Otherwise, it stores and returns the given item.
	// The loaded result is true if the value was loaded, false if stored.
	LoadOrStoreItem(key Itemable) (actual Item, loaded bool)

	// StoreItemIf stores the item in the table only if there is already an item with the given column set to the given
	// value. The ok result indicates if the store occured.
	StoreItemIf(item Itemable, col string, val *dynamodb.AttributeValue) (ok bool)

	// RangeItems calls a consumer sequentially for each item present in the table.
	// If f returns false, range stops the iteration.
	RangeItems(consumer func(Item) bool)
}
