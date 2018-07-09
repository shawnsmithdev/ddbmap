package ddbmap

import (
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/shawnsmithdev/ddbmap/ddbconv"
)

// Item is a type alias for the output of dynamodbattribute.MarshalMap.
// This represents a single row in a DynamoDB table.
type Item map[string]ddb.AttributeValue

// AsItem directly returns this item.
func (item Item) AsItem() Item {
	return item
}

// GetAsBinary returns the attribute of this item with the given name as a byte slice,
// which may be empty if the attribute is not present or not binary data.
func (item Item) GetAsBinary(attr string) []byte {
	return ddbconv.FromBinary(item[attr])
}

// GetAsBinarySet returns the attribute of this item with the given name as a slice of byte slices,
// which may be empty if the attribute is not present or not a binary set.
func (item Item) GetAsBinarySet(attr string) [][]byte {
	return ddbconv.FromBinarySet(item[attr])
}

// GetAsNumber returns the attribute of this item with the given name as an int,
// and will panic if the attribute is not present or is not an integral number.
// If the attribute is optional, use TryGetAsNumber instead.
func (item Item) GetAsNumber(attr string) int {
	return ddbconv.FromNumber(item[attr])
}

// TryGetAsNumber returns the attribute of this item with the given name as an int,
// with a false ok result if the attribute is not present or is not an integral number.
func (item Item) TryGetAsNumber(attr string) (val int, ok bool) {
	return ddbconv.TryFromNumber(item[attr])
}

// GetAsNumberSet returns the attribute of this item with the given name as an int slice,
// which may be empty if the attribute is not present or is not an number set with integral values.
func (item Item) GetAsNumberSet(attr string) []int {
	return ddbconv.FromNumberSet(item[attr])
}

// GetAsString returns the attribute of this imte with the given name as a string,
// which may be empty if the attribute if not present or not a string.
func (item Item) GetAsString(attr string) string {
	return ddbconv.FromString(item[attr])
}
func (item Item) GetAsStringSet(attr string) []string {
	return ddbconv.FromStringSet(item[attr])
}
func (item Item) GetAsBool(attr string) bool {
	return ddbconv.FromBool(item[attr])
}
func (item Item) GetAsMap(attr string) Item {
	return ddbconv.FromMap(item[attr])
}
func (item Item) GetAsList(attr string) []ddb.AttributeValue {
	return ddbconv.FromList(item[attr])
}
func (item Item) IsNull(attr string) bool {
	return *item[attr].NULL
}

// Itemable is implemented by types that can directly build representations of their data in the DynamoDB type system.
// This allows users to take direct control of how their data is presented to DynamoDB.
// Item also implements Itemable, by returning itself, so any method that take Itemable can accept an Item directly.
type Itemable interface {
	AsItem() Item
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

	// StoreItem stores the given item in the table
	StoreItem(item Itemable)
	// StoreItemIfAbsent stores the given item in the table if there is no item already stored with the same key.
	StoreItemIfAbsent(item Itemable) bool

	// LoadOrStoreItem returns the item stored under the given key, if present.
	// Otherwise, it stores and returns the given item.
	// The loaded result is true if the value was loaded, false if stored.
	LoadOrStoreItem(key Itemable) (actual Item, loaded bool)

	// RangeItems calls a consumer sequentially for each item present in the table.
	// If f returns false, range stops the iteration.
	RangeItems(consumer func(Item) bool)
}
