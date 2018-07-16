// Package ddbmap presents a map-like interface for DynamoDB tables.
package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"sync"
	"time"
)

func forbidErr(err error) {
	if err != nil {
		panic(err)
	}
}

// MarshalItem calls dynamodbattribute.MarshalMap on its input and returns the resulting Item.
// This call will panic if MarshalMap returns an error
func MarshalItem(x interface{}) Item {
	xAsMap, err := dynamodbattribute.MarshalMap(x)
	forbidErr(err)
	return xAsMap
}

func UnmarshalItem(item Item, out interface{}) {
	forbidErr(dynamodbattribute.UnmarshalMap(item, out))
}

type ddbmap struct {
	TableConfig
	svc *ddb.DynamoDB
}

func (d *ddbmap) checkErr(err error) {
	if err != nil {
		d.handleErr(err)
	}
}

func (d *ddbmap) handleErr(err error) (dne bool) {
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case ddb.ErrCodeResourceNotFoundException:
			return true
		case ddb.ErrCodeInternalServerError:
			return false
		default:
			d.log(aerr.Error())
		}
	} else {
		d.log(err.Error())
	}
	panic(err) // TODO: don't panic
}

// Logs to aws logger, if present, but only if debug logging is enabled.
func (d *ddbmap) log(v ...interface{}) {
	if d.Config.Logger != nil && d.Config.LogLevel.Matches(aws.LogDebug) {
		d.Config.Logger.Log(v...)
	}
}

func (d *ddbmap) checkExists() {
	if !d.CreateTableIfNotExists {
		return
	}
	// check for table
	for {
		dtReq := d.svc.DescribeTableRequest(&ddb.DescribeTableInput{TableName: &d.TableName})
		dtResp, err := dtReq.Send()
		if err == nil {
			status := dtResp.Table.TableStatus
			if status == ddb.TableStatusActive {
				d.log("[ddbmap] Table exists and is active", dtResp)
				return
			}
			d.log("[ddbmap] Table not yet ready, status:", status)
		} else {
			if d.handleErr(err) {
				break
			}
			d.log("[ddbmap] Failed to describe table, retrying...")
		}
		time.Sleep(1 * time.Second)
	}
	d.createTable()
}

func (d *ddbmap) createTable() {
	// create new table
	schema := []ddb.KeySchemaElement{
		{AttributeName: &d.HashKeyName, KeyType: ddb.KeyTypeHash},
	}
	attrs := []ddb.AttributeDefinition{
		{AttributeName: &d.HashKeyName, AttributeType: d.HashKeyType},
	}
	if d.ranged() {
		schema = append(schema,
			ddb.KeySchemaElement{AttributeName: &d.RangeKeyName, KeyType: ddb.KeyTypeRange})
		attrs = append(attrs,
			ddb.AttributeDefinition{AttributeName: &d.RangeKeyName, AttributeType: d.RangeKeyType})
	}
	if d.CreateTableReadCapacity < 1 {
		d.CreateTableReadCapacity = 1
	}
	if d.CreateTableWriteCapacity < 1 {
		d.CreateTableWriteCapacity = 1
	}
	req := d.svc.CreateTableRequest(&ddb.CreateTableInput{
		TableName:            &d.TableName,
		KeySchema:            schema,
		AttributeDefinitions: attrs,
		ProvisionedThroughput: &ddb.ProvisionedThroughput{
			ReadCapacityUnits:  &d.CreateTableReadCapacity,
			WriteCapacityUnits: &d.CreateTableWriteCapacity,
		},
	})
	d.log("[ddbmap] Will create new table:", d.TableName)
	resp, err := req.Send()
	d.checkErr(err)
	d.log("[ddbmap] Created new table:", resp)
}

func (d *ddbmap) delete(item Item) {
	req := d.svc.DeleteItemRequest(&ddb.DeleteItemInput{
		TableName: &d.TableName,
		Key:       d.ToKeyItem(item),
	})
	_, err := req.Send()
	forbidErr(err)
}

func (d *ddbmap) Delete(key interface{}) {
	d.delete(MarshalItem(key))
}

func (d *ddbmap) DeleteItem(key Itemable) {
	d.delete(key.AsItem())
}

func (d *ddbmap) load(key Item) (value Item, ok bool) {
	req := d.svc.GetItemRequest(&ddb.GetItemInput{
		TableName:      &d.TableName,
		ConsistentRead: &d.ReadWithStrongConsistency,
		Key:            d.ToKeyItem(key),
	})
	resp, err := req.Send()
	forbidErr(err)
	return resp.Item, len(resp.Item) > 0
}

func (d *ddbmap) Load(key interface{}) (value interface{}, ok bool) {
	return d.load(MarshalItem(key))
}

func (d *ddbmap) LoadItem(key Itemable) (item Item, ok bool) {
	return d.load(key.AsItem())
}

func (d *ddbmap) store(item Item) {
	req := d.svc.PutItemRequest(&ddb.PutItemInput{
		TableName: &d.TableName,
		Item:      item,
	})
	_, err := req.Send()
	forbidErr(err)
}

// Stores the given value. The key is ignored.
func (d *ddbmap) Store(_, val interface{}) {
	d.store(MarshalItem(val))
}

func (d *ddbmap) StoreItem(val Itemable) {
	d.store(val.AsItem())
}

func (d *ddbmap) storeItemIfAbsent(item Item) bool {
	return false // TODO
}

func (d *ddbmap) StoreItemIfAbsent(val Itemable) bool {
	return d.storeItemIfAbsent(val.AsItem())
}

// LoadOrStore returns the value stored under same key as the given value, if any,
// else stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (d *ddbmap) loadOrStore(item Item) (Item, bool) {
	for {
		actual, loaded := d.load(item)
		if loaded {
			return actual, loaded
		}
		if d.storeItemIfAbsent(item) {
			return item, false
		}
	}
}

func (d *ddbmap) LoadOrStore(_, val interface{}) (actual interface{}, loaded bool) {
	return d.loadOrStore(MarshalItem(val))
}

func (d *ddbmap) LoadOrStoreItem(val Itemable) (actual Item, loaded bool) {
	return d.loadOrStore(val.AsItem())
}

func (d *ddbmap) StoreItemIfVersion(item Itemable, version int64) (ok bool) {
	return false // TODO:
}

func (d *ddbmap) Range(consumer func(_, value interface{}) bool) {
	d.RangeItems(func(item Item) bool {
		return consumer(nil, item)
	})
}

func (d *ddbmap) rangeSegment(consumer func(Item) bool, workerId int64) {
	var segment *int64
	var totalSegments *int64
	if d.ScanConcurrency > 1 {
		segment = &workerId
		totalSegments = &d.ScanConcurrency
	}
	input := ddb.ScanInput{
		TableName:      &d.TableName,
		ConsistentRead: &d.ReadWithStrongConsistency,
		Select:         ddb.SelectAllAttributes,
		Segment:        segment,
		TotalSegments:  totalSegments,
	}
	for {
		req := d.svc.ScanRequest(&input)
		resp, err := req.Send()
		forbidErr(err)
		for _, item := range resp.Items {
			if consumer(item) {
				return
			}
		}
		if resp.LastEvaluatedKey == nil {
			return
		}
		input.ExclusiveStartKey = resp.LastEvaluatedKey
	}
}

func (d *ddbmap) RangeItems(consumer func(Item) bool) {
	if d.ScanConcurrency > 1 {
		var wg sync.WaitGroup
		for i := int64(0); i < d.ScanConcurrency; i++ {
			wg.Add(1)
			go func(workerId int64) {
				defer wg.Done()
				d.rangeSegment(consumer, workerId)
			}(i)
		}
		wg.Wait()
	} else {
		d.rangeSegment(consumer, 0)
	}
}
