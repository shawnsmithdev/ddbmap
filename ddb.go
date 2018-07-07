// Package ddbmap presents a map-like interface for DynamoDB tables.
package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
)

type TableConfig struct {
	TableName    string
	HashKeyName  string
	RangeKeyName string
}

func (tc TableConfig) ToKeyItem(item Item) Item {
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
		onlyKey := map[string]dynamodb.AttributeValue{
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
	svc *dynamodb.DynamoDB
}

func forbidErr(err error) {
	if err != nil {
		panic(err)
	}
}

func (d *ddbmap) delete(item Item) {
	req := d.svc.DeleteItemRequest(&dynamodb.DeleteItemInput{
		TableName: aws.String(d.TableName),
		Key:       d.ToKeyItem(item),
	})
	_, err := req.Send()
	forbidErr(err)
}

// Marshal calls dynamodbattribute.MarshalMap on its input and returns the resulting Item.
// This call will panic if MarshalMap returns an error
func Marshal(x interface{}) Item {
	xAsMap, err := dynamodbattribute.MarshalMap(x)
	forbidErr(err)
	return xAsMap
}

func (d *ddbmap) Delete(key interface{}) {
	d.delete(Marshal(key))
}

func (d *ddbmap) DeleteItem(key Itemable) {
	d.delete(key.AsItem())
}

func (d *ddbmap) Load(key interface{}) (value interface{}, ok bool) {
	return nil, false
}

func (d *ddbmap) LoadItem(key Itemable) (item Item, ok bool) {
	return nil, false
}

func (d *ddbmap) store(value Item) {
	req := d.svc.PutItemRequest(&dynamodb.PutItemInput{
		TableName: aws.String(d.TableName),
		Item:      value,
	})
	_, err := req.Send()
	forbidErr(err)
}

func (d *ddbmap) Store(_, value interface{}) {
	d.store(Marshal(value))
}

func (d *ddbmap) StoreItem(value Itemable) {
	d.store(value.AsItem())
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

func NewItemMap(awsCfg aws.Config, tableCfg TableConfig) ItemMap {
	return &ddbmap{
		TableConfig: tableCfg,
		svc:         dynamodb.New(awsCfg),
	}
}
