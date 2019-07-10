package ddbmap

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/shawnsmithdev/ddbmap/ddbconv"
	"reflect"
	"sort"
	"strings"
)

// Item is a type underlied by the map type output by dynamodbattribute.MarshalMap.
// This represents a single row in a DynamoDB table or a 'Map' in the DynamoDB type system.
type Item map[string]dynamodb.AttributeValue

// AsItem directly returns this item.
func (item Item) AsItem() Item {
	return item
}

// Exists returns true if the given attribute exists, even if it is null.
func (item Item) Exists(attr string) bool {
	_, ok := item[attr]
	return ok
}

// IsPresent returns true if attribute exists and is not null.
func (item Item) IsPresent(attr string) bool {
	if av, exists := item[attr]; exists {
		return !ddbconv.IsNull(av)
	}
	return false
}

// IsNull returns true if attribute exists, but is null.
func (item Item) IsNull(attr string) bool {
	if av, exists := item[attr]; exists {
		return ddbconv.IsNull(av)
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

// String returns a string representation of the content of the item
func (item Item) String() string {
	// print in order
	var attrs []string
	for k := range item {
		attrs = append(attrs, k)
	}
	sort.Strings(attrs)

	result := "item{"
	for _, k := range attrs {
		v := item[k]
		// attributevalue String() has unwanted newlines and whitespace
		vstr := strings.Replace(v.String(), "\n  ", "", -1)
		vstr = strings.Replace(vstr, "\n", "", -1)
		result = result + fmt.Sprintf("%v:%v, ", k, vstr)
	}
	if len(attrs) > 0 {
		result = result[:len(result)-2]
	}
	return result + "}"
}

// Itemable is implemented by types that can directly build representations of their data in the DynamoDB type system.
// This allows users to take direct control of how their data is presented to DynamoDB.
// Item also implements Itemable, by returning itself, so any method that take Itemable can accept an Item directly.
type Itemable interface {
	AsItem() Item
}

// ItemMap is like Map except that it supports Itemable types and more conditional operations.
type ItemMap interface {
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
	StoreIfAbsent(val interface{}) (stored bool, err error)

	// StoreItemIfAbsent stores the given item if there is no existing item with the same key(s),
	// returning true if stored.
	StoreItemIfAbsent(item Itemable) (stored bool, err error)

	// RangeItems calls the given consumer for each stored item.
	// If the consumer returns false, range eventually stops the iteration.
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

// UnmarshallerForType creates a new ItemUnmashaller function from a template.
// The template may be any value of the struct type you want items to be unmarshalled into, such as the zero value.
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

// MarshalItem will marshal a value into an Item using dynamodbattribute.MarshalMap,
// unless this can be avoided because the value is already an Item or is Itemable.
func MarshalItem(val interface{}) (Item, error) {
	switch valAsType := val.(type) {
	case Item:
		return valAsType, nil
	case Itemable:
		return valAsType.AsItem(), nil
	default:
		return dynamodbattribute.MarshalMap(val)
	}
}
