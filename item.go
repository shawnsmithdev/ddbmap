package ddbmap

import (
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/shawnsmithdev/ddbmap/ddbconv"
	"reflect"
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

// GetAsInt returns the attribute of this item with the given name as an int,
// and will panic if the attribute is not present or is not an integral number.
// If the attribute is optional, use TryGetAsNumber instead.
func (item Item) GetAsInt(attr string) int {
	return ddbconv.FromInt(item[attr])
}

// TryGetAsInt returns the attribute of this item with the given name as an int,
// with a false ok result if the attribute is not present or is not an integral number.
func (item Item) TryGetAsInt(attr string) (val int, ok bool) {
	if n, present := item[attr]; present {
		return ddbconv.TryFromInt(n)
	}
	return 0, false
}

// GetAsIntSet returns the attribute of this item with the given name as an slice of ints,
// which may be empty if the attribute is not present or is not an number set with integral values.
func (item Item) GetAsIntSet(attr string) []int {
	if n, present := item[attr]; present {
		return ddbconv.FromIntSet(n)
	}
	return []int{}
}

// GetAsString returns the attribute of this item with the given name as a string,
// which may be empty if the attribute if not present or not a string.
func (item Item) GetAsString(attr string) string {
	return ddbconv.FromString(item[attr])
}

// GetAsStringSet returns the attribute of this item with the given name as a slice of strings,
// which may be empty if the attribute if not present or not a string set.
func (item Item) GetAsStringSet(attr string) []string {
	return ddbconv.FromStringSet(item[attr])
}

// GetAsBool returns the attribute of this item with the given name as a bool,
// and will panic if the attribute is not present or not a boolean.
// If the attribute is optional, use TryGetAsBoolean instead
func (item Item) GetAsBool(attr string) bool {
	return ddbconv.FromBool(item[attr])
}

// TryGetAsBool returns the attribute of this item with the given name as a bool.
// The ok result returns true if the value was present and a boolean.
func (item Item) TryGetAsBool(attr string) (val bool, ok bool) {
	if b, present := item[attr]; present {
		return ddbconv.TryFromBool(b)
	}
	return false, false
}

// GetAsMap TODO: doc
func (item Item) GetAsMap(attr string) Item {
	return ddbconv.FromMap(item[attr])
}

// GetAsList TODO: doc
func (item Item) GetAsList(attr string) []ddb.AttributeValue {
	return ddbconv.FromList(item[attr])
}

// Exists returns true if the given attribute exists, even if it is null.
func (item Item) Exists(attr string) bool {
	_, ok := item[attr]
	return ok
}

// IsPresent returns true if attribute exists and is not null.
func (item Item) IsPresent(attr string) bool {
	if av, exists := item[attr]; exists {
		null := av.NULL
		return null == nil || !*null
	}
	return false
}

// IsNull returns true if attribute exists, but is null.
func (item Item) IsNull(attr string) bool {
	if av, exists := item[attr]; exists {
		null := av.NULL
		return null != nil && *null
	}
	return false
}

// Project returns a new item based on this one, but with only the specified attributes.
func (item Item) Project(attrs ...string) Item {
	result := make(Item, len(attrs))
	for _, attr := range attrs {
		if val, ok := item[attr]; ok {
			result[attr] = val
		}
	}
	return result
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

	// StoreIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
	// Returns true if the item was stored.
	StoreIfVersion(val interface{}, version int64) (ok bool)

	// StoreItemIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
	// Returns true if the item was stored.
	StoreItemIfVersion(item Itemable, version int64) (ok bool, err error)
}

// ItemUnmarshaller is a function that can convert an Item into some other type
type ItemUnmarshaller func(Item) (interface{}, error)

// UnmarshallerForType creates a new ItemUnmashaller function that uses reflection and dynamodbattribute.UnmarshalMap.
// The template should be a value of the struct type you want items to be unmarshalled into.
func UnmarshallerForType(template interface{}) ItemUnmarshaller {
	t := reflect.TypeOf(template)
	return func(item Item) (interface{}, error) {
		val := reflect.New(t).Interface()
		if err := dynamodbattribute.UnmarshalMap(item, val); err != nil {
			return nil, err
		}
		return reflect.ValueOf(val).Elem().Interface(), nil
	}
}
