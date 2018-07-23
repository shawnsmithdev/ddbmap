// Package ddbmap presents a map-like interface for DynamoDB tables.
package ddbmap

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
	"log"
	"sync"
)

func getCode(err error) string {
	if aerr, ok := err.(awserr.Error); ok {
		return aerr.Code()
	}
	return ""
}

// Only use if documented to panic or when err can only be due to a library bug
func forbidErr(err error, logger aws.LoggerFunc) {
	if err != nil {
		logErr(err, logger)
		logger("unhandled error, will now panic")
		panic(err)
	}
}

func marshalItem(x interface{}) (Item, error) {
	switch xAsType := x.(type) {
	case Itemable:
		return xAsType.AsItem(), nil
	default:
		return dynamodbattribute.MarshalMap(x)
	}
}

// DdbMap implements ItemMap as pointer methods, backed by a DynamoDB table.
type DdbMap struct {
	TableConfig
	Client *ddb.DynamoDB
}

func (d *DdbMap) log(vals ...interface{}) {
	var logger aws.LoggerFunc
	if d.Logger == nil {
		if d.AWSConfig.Logger == nil {
			logger = log.Println
		} else {
			logger = d.AWSConfig.Logger.Log
		}
	} else {
		logger = d.Logger.Log
	}
	logger(append([]interface{}{"(ddbmap)"}, vals...)...)
}

func logErr(err error, logger aws.LoggerFunc) {
	e := err
	for {
		logger(e.Error())
		if aerr, ok := e.(awserr.Error); ok {
			if aerr.OrigErr() == nil {
				return
			}
			logger("caused by:")
			e = aerr.OrigErr()
		} else {
			return
		}
	}
}

// Only use if documented to panic or when err can only be due to a library bug
func (d *DdbMap) forbidErr(err error) {
	forbidErr(err, d.log)
}

func (d *DdbMap) debug(vals ...interface{}) {
	if d.Debug {
		d.log(vals...)
	}
}

// check table description, optionally using result to set key configuration, returning true if table is active.
func (d *DdbMap) describeTable(setKeys bool) (active bool, err error) {
	input := &ddb.DescribeTableInput{TableName: &d.TableName}
	d.debug("describe table request input:", input)
	dtReq := d.Client.DescribeTableRequest(input)
	dtResp, err := dtReq.Send()
	d.debug("describe table response:", dtResp, ", error:", err)
	if err != nil {
		if ddb.ErrCodeResourceNotFoundException == getCode(err) {
			return false, nil
		}
		return false, err
	}
	status := dtResp.Table.TableStatus
	d.debug("table status:", status)
	active = status == ddb.TableStatusActive
	if active {
		if setKeys && "" == d.HashKeyName {
			for _, keySchema := range dtResp.Table.KeySchema {
				if keySchema.KeyType == ddb.KeyTypeHash {
					d.HashKeyName = *keySchema.AttributeName
					d.debug("found hash key:", d.HashKeyName)
				} else {
					d.RangeKeyName = *keySchema.AttributeName
					d.debug("found range key:", d.RangeKeyName)
				}
			}
		}
	}
	return active, nil
}

// creates a new table
func (d *DdbMap) createTable() error {
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
	input := &ddb.CreateTableInput{
		TableName:            &d.TableName,
		KeySchema:            schema,
		AttributeDefinitions: attrs,
		ProvisionedThroughput: &ddb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(int64(d.CreateTableReadCapacity)),
			WriteCapacityUnits: aws.Int64(int64(d.CreateTableWriteCapacity)),
		},
	}
	d.debug("create table request input:", input)
	resp, err := d.Client.CreateTableRequest(input).Send()
	d.debug("created table response:", resp, ", error:", err)
	return err
}

func (d *DdbMap) delete(item Item) error {
	input := &ddb.DeleteItemInput{
		TableName: &d.TableName,
		Key:       d.ToKeyItem(item),
	}
	d.debug("delete request input:", input)
	resp, err := d.Client.DeleteItemRequest(input).Send()
	d.debug("delete response:", resp, ", error:", err)
	return err
}

func (d *DdbMap) Delete(key interface{}) {
	item, err := marshalItem(key)
	d.forbidErr(err)
	d.forbidErr(d.delete(item))
}

func (d *DdbMap) DeleteItem(key Itemable) error {
	return d.delete(key.AsItem())
}

func (d *DdbMap) load(key Item) (value Item, ok bool, err error) {
	input := &ddb.GetItemInput{
		TableName:      &d.TableName,
		ConsistentRead: &d.ReadWithStrongConsistency,
		Key:            d.ToKeyItem(key),
	}
	d.debug("load request input:", input)
	resp, err := d.Client.GetItemRequest(input).Send()
	d.debug("load response:", resp, ", error:", err)
	if err == nil {
		return resp.Item, len(resp.Item) > 0, err
	}
	return nil, false, err
}

func (d *DdbMap) Load(key interface{}) (value interface{}, ok bool) {
	keyItem, err := marshalItem(key)
	d.forbidErr(err)
	result, ok, err2 := d.load(keyItem)
	d.forbidErr(err2)
	return result, ok
}

func (d *DdbMap) LoadItem(key Itemable) (item Item, ok bool, err error) {
	return d.load(key.AsItem())
}

func (d *DdbMap) store(item Item, condition *expression.ConditionBuilder) error {
	input := &ddb.PutItemInput{
		TableName: &d.TableName,
		Item:      item,
	}
	if condition != nil {
		condExpr, _ := expression.NewBuilder().WithCondition(*condition).Build()
		input.ConditionExpression = condExpr.Condition()
	}
	d.debug("store request input:", input)
	resp, err := d.Client.PutItemRequest(input).Send()
	d.debug("store response:", resp, ", error:", err)
	return err
}

// Stores the given value. The key is ignored.
func (d *DdbMap) Store(_, val interface{}) {
	valItem, err := marshalItem(val)
	d.forbidErr(err)
	d.forbidErr(d.store(valItem, nil))
}

func (d *DdbMap) StoreItem(val Itemable) error {
	return d.store(val.AsItem(), nil)
}

func (d *DdbMap) storeItemIfAbsent(item Item) (stored bool, err error) {
	noKey := expression.Name(d.HashKeyName).AttributeNotExists()
	err = d.store(item, &noKey)
	if err == nil {
		return true, nil
	}
	if ddb.ErrCodeConditionalCheckFailedException != getCode(err) {
		return false, err
	}
	return false, nil
}

// StoreIfAbsent stores the given value if there is no existing value with the same key(s),
// returning true if stored.
func (d *DdbMap) StoreIfAbsent(_, val interface{}) (stored bool) {
	valItem, err := marshalItem(val)
	d.forbidErr(err)
	stored, err2 := d.storeItemIfAbsent(valItem)
	d.forbidErr(err2)
	return stored
}

// StoreItemIfAbsent stores the given item if there is no existing item with the same key(s),
// returning true if stored.
func (d *DdbMap) StoreItemIfAbsent(val Itemable) (stored bool, err error) {
	return d.storeItemIfAbsent(val.AsItem())
}

// LoadOrStore returns the value stored under same key as the given value, if any,
// else stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (d *DdbMap) loadOrStore(item Item) (Item, bool, error) {
	for {
		if result, loaded, err := d.load(item); loaded || err != nil {
			return result, loaded, err
		}
		if stored, err := d.storeItemIfAbsent(item); stored || err != nil {
			return item, !stored, err
		}
	}
}

func (d *DdbMap) LoadOrStore(_, val interface{}) (interface{}, bool) {
	valItem, err := marshalItem(val)
	d.forbidErr(err)
	actual, stored, err2 := d.loadOrStore(valItem)
	d.forbidErr(err2)
	return actual, stored
}

func (d *DdbMap) LoadOrStoreItem(val Itemable) (actual Item, loaded bool, err error) {
	return d.loadOrStore(val.AsItem())
}

func (d *DdbMap) storeItemIfVersion(item Item, version int64) (bool, error) {
	hasVersion := expression.Name(d.VersionName).Equal(expression.Value(version))
	err := d.store(item.AsItem(), &hasVersion)
	if ddb.ErrCodeConditionalCheckFailedException == getCode(err) {
		return false, nil
	}
	return err == nil, err
}

// StoreIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
// Returns true if the item was stored.
func (d *DdbMap) StoreIfVersion(val interface{}, version int64) (ok bool) {
	valItem, err := marshalItem(val)
	d.forbidErr(err)
	ok, err2 := d.storeItemIfVersion(valItem, version)
	d.forbidErr(err2)
	return ok
}

// StoreItemIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
// Returns true if the item was stored.
func (d *DdbMap) StoreItemIfVersion(item Itemable, version int64) (ok bool, err error) {
	return d.storeItemIfVersion(item.AsItem(), version)
}

func (d *DdbMap) rangeSegment(consumer func(Item) bool, workerId int) error {
	var segment *int64
	var totalSegments *int64
	if d.ScanConcurrency > 1 {
		segment = aws.Int64(int64(workerId))
		totalSegments = aws.Int64(int64(d.ScanConcurrency))
	}
	input := &ddb.ScanInput{
		TableName:      &d.TableName,
		ConsistentRead: &d.ReadWithStrongConsistency,
		Select:         ddb.SelectAllAttributes,
		Segment:        segment,
		TotalSegments:  totalSegments,
	}
	for {
		d.debug("scan request input:", input, ", worker:", workerId)
		resp, err := d.Client.ScanRequest(input).Send()
		d.debug("scan response:", resp, ", worker:", workerId, ", error:", err)
		if err != nil {
			return err
		}
		for _, item := range resp.Items {
			if !consumer(item) {
				return nil
			}
		}
		if resp.LastEvaluatedKey == nil {
			return nil
		}
		input.ExclusiveStartKey = resp.LastEvaluatedKey
	}
}

func (d *DdbMap) Range(consumer func(_, value interface{}) bool) {
	d.RangeItems(func(item Item) bool {
		return consumer(nil, item)
	})
}

func (d *DdbMap) RangeItems(consumer func(Item) bool) error {
	if d.ScanConcurrency > 1 {
		scanCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		workerErrs := make(chan error)
		var errOnce sync.Once
		var wg sync.WaitGroup
		for i := int(0); i < d.ScanConcurrency; i++ {
			wg.Add(1)
			go func(workerId int) {
				defer wg.Done()
				d.debug("starting scan worker: ", workerId)
				awareConsumer := func(items Item) bool {
					getMore := consumer(items)
					if !getMore {
						d.debug("scan consumer stopped iteration early, worker: ", workerId)
						cancel()
					}
					return getMore && scanCtx.Err() == nil
				}
				if err := d.rangeSegment(awareConsumer, workerId); err != nil {
					d.debug("scan worker had error:", err, ", worker:", workerId)
					errOnce.Do(func() { workerErrs <- err })
				} else {
					d.debug("scan worker done, worker:", workerId)
				}
			}(i)
		}
		go func() {
			wg.Wait()
			if scanCtx.Err() == nil {
				close(workerErrs)
			}
		}()
		select {
		case <-scanCtx.Done():
			return nil
		case err := <-workerErrs:
			return err
		}
	}
	return d.rangeSegment(consumer, 0)
}
