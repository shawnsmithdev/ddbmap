// Package ddbmap presents a map-like interface for DynamoDB tables.
package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
	"sync"
)

var (
	stdOutLogger = aws.NewDefaultLogger().Log // TODO: this may be moved by upstream later
)

// Only use if documented to panic or when err can only be due to a library bug
func forbidErr(err error) {
	if err != nil {
		logErr(err, stdOutLogger)
		panic(err)
	}
}

// MarshalItem calls dynamodbattribute.MarshalMap on its input and returns the resulting Item.
// It is different in that it does not return an error, but will panic if given value cannot be marshaled.
func MarshalItem(x interface{}) Item {
	xAsMap, err := dynamodbattribute.MarshalMap(x)
	forbidErr(err)
	return xAsMap
}

// UnmarshalItem calls dynamodbattribute.UnmarshalMap with given input item and output pointer.
// It is different in that it does not return an error, but will panic if given value cannot be unmarshaled.
func UnmarshalItem(item Item, out interface{}) {
	forbidErr(dynamodbattribute.UnmarshalMap(item, out))
}

type ddbmap struct {
	TableConfig
	svc *ddb.DynamoDB // TODO: Configurable?
}

// Only use if documented to panic or when err can only be due to a library bug
func (d *ddbmap) forbidErr(err error) {
	if err != nil {
		logErr(err, d.log)
		d.log("Unhandled error, will now panic")
		panic(err)
	}
}

func logErr(err error, logger aws.LoggerFunc) {
	e := err
	for {
		logger.Log(e.Error())
		if aerr, ok := e.(awserr.Error); ok {
			if aerr.OrigErr() == nil {
				return
			}
			logger.Log("caused by:")
			e = aerr.OrigErr()
		} else {
			return
		}
	}
}

func getCode(err error) string {
	if aerr, ok := err.(awserr.Error); ok {
		return aerr.Code()
	}
	return ""
}

func (d *ddbmap) log(v ...interface{}) {
	if d.Debug {
		var logger aws.LoggerFunc
		if d.Logger == nil {
			if d.AWSConfig.Logger == nil {
				logger = stdOutLogger
			} else {
				logger = d.AWSConfig.Logger.Log
			}
		} else {
			logger = d.Logger.Log
		}
		logger(append([]interface{}{"(ddbmap)"}, v...)...)
	}
}

// check table description, optionally using result to set key configuration, returning true if table exists.
func (d *ddbmap) describeTable(setKeys bool) bool {
	dtReq := d.svc.DescribeTableRequest(&ddb.DescribeTableInput{TableName: &d.TableName})
	dtResp, err := dtReq.Send()
	if err != nil {
		if ddb.ErrCodeResourceNotFoundException == getCode(err) {
			return false
		}
		d.forbidErr(err)
	}
	status := dtResp.Table.TableStatus
	active := status == ddb.TableStatusActive
	if active {
		d.log("Table exists and is active:", dtResp)
		if setKeys && "" == d.HashKeyName {
			for _, keySchema := range dtResp.Table.KeySchema {
				if keySchema.KeyType == ddb.KeyTypeHash {
					d.HashKeyName = *keySchema.AttributeName
					d.log("Found hash key:", d.HashKeyName)
				} else {
					d.RangeKeyName = *keySchema.AttributeName
					d.log("Found range key:", d.RangeKeyName)
				}
			}
		}
	} else {
		d.log("Table not yet ready, status:", status)
	}
	return active
}

// creates a new table
func (d *ddbmap) createTable() {
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
			ReadCapacityUnits:  aws.Int64(int64(d.CreateTableReadCapacity)),
			WriteCapacityUnits: aws.Int64(int64(d.CreateTableWriteCapacity)),
		},
	})
	d.log("Will create new table:", d.TableName)
	resp, err := req.Send()
	d.forbidErr(err)
	d.log("Created new table:", resp)
}

func (d *ddbmap) delete(item Item) {
	req := d.svc.DeleteItemRequest(&ddb.DeleteItemInput{
		TableName: &d.TableName,
		Key:       d.ToKeyItem(item),
	})
	_, err := req.Send()
	d.forbidErr(err) // TODO
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
	d.forbidErr(err) // TODO
	return resp.Item, len(resp.Item) > 0
}

func (d *ddbmap) Load(key interface{}) (value interface{}, ok bool) {
	return d.load(MarshalItem(key))
}

func (d *ddbmap) LoadItem(key Itemable) (item Item, ok bool) {
	return d.load(key.AsItem())
}

func (d *ddbmap) store(item Item, cond *expression.ConditionBuilder) error {
	input := &ddb.PutItemInput{
		TableName: &d.TableName,
		Item:      item,
	}
	if cond != nil {
		condExpr, _ := expression.NewBuilder().WithCondition(*cond).Build()
		input.ConditionExpression = condExpr.Condition()
	}
	req := d.svc.PutItemRequest(input)
	_, err := req.Send()
	return err
}

// Stores the given value. The key is ignored.
func (d *ddbmap) Store(_, val interface{}) {
	d.forbidErr(d.store(MarshalItem(val), nil))
}

func (d *ddbmap) StoreItem(val Itemable) {
	d.forbidErr(d.store(val.AsItem(), nil))
}

func (d *ddbmap) storeItemIfAbsent(item Item) bool {
	noKey := expression.Name(d.HashKeyName).AttributeNotExists()
	err := d.store(item, &noKey)
	if err == nil {
		return true
	}
	if ddb.ErrCodeConditionalCheckFailedException != getCode(err) {
		d.forbidErr(err) // TODO return it?
	}
	return false
}

// StoreItemIfAbsent stores the given item if there is no existing item with the same key(s),
// returning true if stored.
func (d *ddbmap) StoreItemIfAbsent(val Itemable) bool {
	return d.storeItemIfAbsent(val.AsItem())
}

// StoreIfAbsent stores the given value if there is no existing value with the same key(s),
// returning true if stored.
func (d *ddbmap) StoreIfAbsent(_, val interface{}) bool {
	return d.storeItemIfAbsent(MarshalItem(val))
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

func (d *ddbmap) storeItemIfVersion(item Item, version int64) bool {
	hasVersion := expression.Name(d.VersionName).Equal(expression.Value(version))
	err := d.store(item.AsItem(), &hasVersion)
	if err == nil {
		return true
	}
	if ddb.ErrCodeConditionalCheckFailedException != getCode(err) {
		d.forbidErr(err)
	}
	return false
}

// StoreIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
// Returns true if the item was stored.
func (d *ddbmap) StoreIfVersion(val interface{}, version int64) (ok bool) {
	return d.storeItemIfVersion(MarshalItem(val), version)
}

// StoreItemIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
// Returns true if the item was stored.
func (d *ddbmap) StoreItemIfVersion(item Itemable, version int64) (ok bool) {
	return d.storeItemIfVersion(item.AsItem(), version)
}

func (d *ddbmap) rangeSegment(consumer func(Item) bool, workerId int) error {
	var segment *int64
	var totalSegments *int64
	if d.ScanConcurrency > 1 {
		segment = aws.Int64(int64(workerId))
		totalSegments = aws.Int64(int64(d.ScanConcurrency))
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
		if err != nil {
			return err
		}
		for _, item := range resp.Items {
			if consumer(item) {
				return nil
			}
		}
		if resp.LastEvaluatedKey == nil {
			return nil
		}
		input.ExclusiveStartKey = resp.LastEvaluatedKey
	}
}

func (d *ddbmap) Range(consumer func(_, value interface{}) bool) {
	d.RangeItems(func(item Item) bool {
		return consumer(nil, item)
	})
}

func (d *ddbmap) RangeItems(consumer func(Item) bool) {
	if d.ScanConcurrency > 1 {
		var wg sync.WaitGroup
		for i := int(0); i < d.ScanConcurrency; i++ {
			wg.Add(1)
			go func(workerId int) {
				defer wg.Done()
				err := d.rangeSegment(consumer, workerId)
				d.forbidErr(err) // TODO
			}(i)
		}
		wg.Wait()
	} else {
		d.rangeSegment(consumer, 0)
	}
}
