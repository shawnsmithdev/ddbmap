package ddbmap

import (
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/shawnsmithdev/ddbmap/ddbconv"
)

// Item is a type alias for map[string]AttributeValue, the output of dynamodbattribute.MarshalMap.
// This represents a single row in a DynamoDB table or a 'Map' in the DynamoDB type system.
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

// ItemMap is a Map that supports Itemable types and more conditional operations.
// Note that DynamoDB key(s) include a hash key, and optionally a range key.
type ItemMap interface {
	Map

	// DeleteItem deletes any existing item with the same key(s) as the given item.
	DeleteItem(keys Itemable) error

	// LoadItem returns the existing item, if present, with the same key(s) as the given item.
	// The ok result returns true if the value was found.
	LoadItem(keys Itemable) (item Item, ok bool, err error)

	// StoreItem stores the given item, clobbering any existing item with the same key(s).
	StoreItem(item Itemable) error

	// LoadOrStoreItem returns the existing item, if present, with the same key(s) as the given item.
	// Otherwise, it stores and returns the given item.
	// The loaded result is true if the value was loaded, false if stored.
	LoadOrStoreItem(item Itemable) (actual Item, loaded bool, err error)

	// StoreIfAbsent stores the given value if there is no existing value with the same key(s),
	// returning true if stored.
	StoreIfAbsent(key, val interface{}) bool

	// StoreItemIfAbsent stores the given item if there is no existing item with the same key(s),
	// returning true if stored.
	StoreItemIfAbsent(item Itemable) (stored bool, err error)

	// RangeItems calls the given consumer for each stored item.
	// If the consumer returns false, range eventually stops the iteration.
	// If a consumer returns false once, it should eventually always return false.
	RangeItems(consumer func(Item) (resume bool)) error
}

// VersionedItemMap is an ItemMap that includes some compare-and-swap methods using a configured int64 version field.
type VersionedItemMap interface {
	ItemMap
	// StoreIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
	// Returns true if the item was stored.
	StoreIfVersion(val interface{}, version int64) (ok bool)
	// StoreItemIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
	// Returns true if the item was stored.
	StoreItemIfVersion(item Itemable, version int64) (ok bool)
}
