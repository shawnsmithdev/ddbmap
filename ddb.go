// Package ddbmap presents a map-like interface for DynamoDB tables.
package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
)

func forbidErr(err error) {
	if err != nil {
		panic(err)
	}
}

// Marshal calls dynamodbattribute.MarshalMap on its input and returns the resulting Item.
// This call will panic if MarshalMap returns an error
func Marshal(x interface{}) Item {
	xAsMap, err := dynamodbattribute.MarshalMap(x)
	forbidErr(err)
	return xAsMap
}

type TableConfig struct {
	aws.Config
	TableName                 string
	HashKeyName               string
	RangeKeyName              string
	ScanConcurrency           int
	ReadWithStrongConsistency bool
}

func (tc TableConfig) ToKeyItem(item Item) Item {
	// TODO: This is kinda iffy isn't it...
	switch len(item) {
	case 0:
		panic("empty item cannot be key")
	case 1:
		return item
	case 2:
		if len(tc.RangeKeyName) > 0 {
			return item
		}
		fallthrough
	default:
		onlyKey := Item{
			tc.HashKeyName: item[tc.HashKeyName],
		}
		if len(tc.RangeKeyName) > 0 {
			onlyKey[tc.RangeKeyName] = item[tc.RangeKeyName]
		}
		return onlyKey
	}
}

type ddbmap struct {
	TableConfig
	svc *ddb.DynamoDB
}

func (d *ddbmap) delete(item Item) {
	req := d.svc.DeleteItemRequest(&ddb.DeleteItemInput{
		TableName: aws.String(d.TableName),
		Key:       d.ToKeyItem(item),
	})
	_, err := req.Send()
	forbidErr(err)
}

func (d *ddbmap) Delete(key interface{}) {
	d.delete(Marshal(key))
}

func (d *ddbmap) DeleteItem(key Itemable) {
	d.delete(key.AsItem())
}

func (d *ddbmap) load(key Item) (value Item, ok bool) {
	req := d.svc.GetItemRequest(&ddb.GetItemInput{
		TableName:      aws.String(d.TableName),
		ConsistentRead: aws.Bool(d.ReadWithStrongConsistency),
		Key:            d.ToKeyItem(key),
	})
	resp, err := req.Send()
	forbidErr(err)
	return resp.Item, len(resp.Item) > 0
}

func (d *ddbmap) Load(key interface{}) (value interface{}, ok bool) {
	return d.load(Marshal(key))
}

func (d *ddbmap) LoadItem(key Itemable) (item Item, ok bool) {
	return d.load(key.AsItem())
}

func (d *ddbmap) store(value Item) {
	req := d.svc.PutItemRequest(&ddb.PutItemInput{
		TableName: aws.String(d.TableName),
		Item:      value,
	})
	_, err := req.Send()
	forbidErr(err)
}

// Stores the given value. The key is ignored.
func (d *ddbmap) Store(_, value interface{}) {
	d.store(Marshal(value))
}

func (d *ddbmap) StoreItem(value Itemable) {
	d.store(value.AsItem())
}

func (d *ddbmap) storeItemIfAbsent(value Item) bool {
	return false // TODO
}

func (d *ddbmap) StoreItemIfAbsent(value Itemable) bool {
	return d.storeItemIfAbsent(value.AsItem())
}

// LoadOrStore returns the value stored under same key as the given value, if any,
// else stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (d *ddbmap) loadOrStore(val Item) (Item, bool) {
	for {
		actual, loaded := d.load(val)
		if loaded {
			return actual, loaded
		}
		if d.storeItemIfAbsent(val) {
			return val, false
		}
	}
}

func (d *ddbmap) LoadOrStore(_, val interface{}) (actual interface{}, loaded bool) {
	return d.loadOrStore(Marshal(val))
}

func (d *ddbmap) LoadOrStoreItem(val Itemable) (actual Item, loaded bool) {
	return d.loadOrStore(val.AsItem())
}

func (d *ddbmap) StoreItemIf(item Itemable, col string, val ddb.AttributeValue) (ok bool) {
	return false // TODO
}

func (d *ddbmap) Range(f func(key, value interface{}) bool) {
	// TODO
}

func (d *ddbmap) RangeItems(consumer func(Item) bool) {
	// TODO
}

func DynamoItemMap(tableCfg TableConfig) ItemMap {
	return &ddbmap{
		TableConfig: tableCfg,
		svc:         ddb.New(tableCfg.Config),
	}
}
