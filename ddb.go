// Package ddbmap presents a map-like interface for DynamoDB tables.
package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Item map[string]*dynamodb.AttributeValue

type Itemable interface {
	AsItem() Item
}

// Map is the interface of sync.Map. For DynamoDB, the key and the value are both Items.
// Code that uses this interface can use sync.Map and DynamoDB interchangebly.
type Map interface {
	Delete(key interface{})
	Load(key interface{}) (value interface{}, ok bool)
	LoadOrStore(key, value interface{}) (actual interface{}, loaded bool)
	Range(f func(key, value interface{}) bool)
	Store(key, value interface{})
}

// Note that a "key" in DynamoDB is the hash key, and, if the table so configured, the range key.
type DdbMap interface {
	Map
	// Delete deletes the item with the same key as the given item, if it exists.
	DeleteItem(key Itemable)
	// Load returns the item stored under the same key as the given item, if present.
	// The ok result indicates whether the value was found.
	LoadItem(key Itemable) (item Item, ok bool)
	// Store stores the item in the table
	StoreItem(item Itemable)
	// LoadOrStore returns the item stored under the same key as the given item, if present.
	// Otherwise, it stores and returns the given item.
	// The loaded result is true if the value was loaded, false if stored.
	LoadOrStoreItem(key Itemable) (actual Item, loaded bool)
	// StoreIf stores the item in the table only if there is already an item with the given column set to the given
	// value. The ok result indicates if the store occured.
	StoreItemIf(item Itemable, col string, val *dynamodb.AttributeValue) (ok bool)
	// Range calls a consumer sequentially for each item present in the table.
	// If f returns false, range stops the iteration.
	RangeItems(consumer func(Item) bool)
}

type ddbmap struct {
	svc   *dynamodb.DynamoDB
	table string
}

func (d *ddbmap) Delete(key interface{}) {
}

func (d *ddbmap) DeleteItem(key Itemable) {
}
func (d *ddbmap) Load(key interface{}) (value interface{}, ok bool) {
	return nil, false
}

func (d *ddbmap) LoadItem(key Itemable) (item Item, ok bool) {
	return nil, false
}

func (d *ddbmap) Store(key, value interface{}) {
}

func (d *ddbmap) StoreItem(key Itemable) {
}

func (d *ddbmap) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool) {
	return nil, false
}

func (d *ddbmap) LoadOrStoreItem(key Itemable) (actual Item, loaded bool) {
	return nil, false
}

func (d *ddbmap) StoreItemIf(item Itemable, col string, val *dynamodb.AttributeValue) (ok bool) {
	return false
}
func (d *ddbmap) Range(f func(key, value interface{}) bool) {
}

func (d *ddbmap) RangeItems(consumer func(Item) bool) {
}

func NewDdbMap(cfg aws.Config, table string) DdbMap {
	svc := dynamodb.New(cfg)

	return &ddbmap{
		svc:   svc,
		table: table,
	}
}
