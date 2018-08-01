// Package ddbmap presents a map-like interface for DynamoDB tables.
package ddbmap

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
	"golang.org/x/sync/errgroup"
	"log"
)

var (
	// Indicates that a range operation consumer caused an early termination by returning false, do not return it.
	earlyTermination = fmt.Errorf("ddbmap early termination")
)

// DynamoMap implements ItemMap (with pointer methods), backed by a DynamoDB table.
// The reflection-based api (ddb.Map) will panic on any unhandled AWS client error.
// The Itemable api (ddb.ItemMap) returns errors instead and so will not panic.
type DynamoMap struct {
	TableConfig
	Client *ddb.DynamoDB
}

func (d *DynamoMap) log(vals ...interface{}) {
	toLog := append([]interface{}{"(ddbmap)"}, vals...)
	if d.Logger == nil {
		log.Println(toLog...)
	} else {
		d.Logger.Log(toLog...)
	}
}

// Only use if documented to panic or when err can only be due to a library bug
func (d *DynamoMap) forbidErr(err error) {
	forbidErr(err, d.log)
}

func (d *DynamoMap) debug(vals ...interface{}) {
	if d.Debug {
		d.log(vals...)
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

func (d *DynamoMap) unmarshalItem(item Item) interface{} {
	// do not unmarshal if user has not configured an unmarshaller
	if d.ValueUnmarshaller == nil {
		return item
	}
	result, err := d.ValueUnmarshaller(item)
	d.forbidErr(err)
	return result
}

// check table description, optionally using result to set key configuration, returning true if table is active.
func (d *DynamoMap) describeTable(setKeys bool) (active bool, err error) {
	input := &ddb.DescribeTableInput{TableName: &d.TableName}
	d.debug("describe table request input:", input)
	dtReq := d.Client.DescribeTableRequest(input)
	dtResp, err := dtReq.Send()
	d.debug("describe table response:", dtResp, ", error:", err)
	if err != nil {
		if ddb.ErrCodeResourceNotFoundException == getErrCode(err) {
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
func (d *DynamoMap) createTable() error {
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

func (d *DynamoMap) delete(item Item) error {
	input := &ddb.DeleteItemInput{
		TableName: &d.TableName,
		Key:       d.ToKeyItem(item),
	}
	d.debug("delete request input:", input)
	resp, err := d.Client.DeleteItemRequest(input).Send()
	d.debug("delete response:", resp, ", error:", err)
	return err
}

func (d *DynamoMap) DeleteItem(key Itemable) error {
	return d.delete(key.AsItem())
}

func (d *DynamoMap) Delete(key interface{}) {
	item, err := MarshalItem(key)
	d.forbidErr(err)
	d.forbidErr(d.delete(item))
}

func (d *DynamoMap) load(key Item) (value Item, ok bool, err error) {
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

func (d *DynamoMap) LoadItem(key Itemable) (item Item, ok bool, err error) {
	return d.load(key.AsItem())
}

func (d *DynamoMap) Load(key interface{}) (value interface{}, ok bool) {
	keyItem, err := MarshalItem(key)
	d.forbidErr(err)
	resultItem, ok, err := d.load(keyItem)
	d.forbidErr(err)
	return d.unmarshalItem(resultItem), ok
}

func (d *DynamoMap) store(item Item, condition *expression.ConditionBuilder) error {
	input := &ddb.PutItemInput{
		TableName: &d.TableName,
		Item:      item,
	}
	if condition != nil {
		condExpr, err := expression.NewBuilder().WithCondition(*condition).Build()
		if err != nil {
			return err
		}
		input.ConditionExpression = condExpr.Condition()
	}
	d.debug("store request input:", input)
	resp, err := d.Client.PutItemRequest(input).Send()
	d.debug("store response:", resp, ", error:", err)
	return err
}

func (d *DynamoMap) StoreItem(val Itemable) error {
	return d.store(val.AsItem(), nil)
}

// Stores the given value. The first argument is ignored.
func (d *DynamoMap) Store(_, val interface{}) {
	valItem, err := MarshalItem(val)
	d.forbidErr(err)
	d.forbidErr(d.store(valItem, nil))
}
func (d *DynamoMap) storeItemIfAbsent(item Item) (stored bool, err error) {
	noKey := expression.Name(d.HashKeyName).AttributeNotExists()
	err = d.store(item, &noKey)
	if err == nil {
		return true, nil
	}
	if ddb.ErrCodeConditionalCheckFailedException != getErrCode(err) {
		return false, err
	}
	return false, nil
}

// StoreItemIfAbsent stores the given item if there is no existing item with the same key(s),
// returning true if stored.
func (d *DynamoMap) StoreItemIfAbsent(val Itemable) (stored bool, err error) {
	return d.storeItemIfAbsent(val.AsItem())
}

// StoreIfAbsent stores the given value if there is no existing value with the same key(s),
// returning true if stored. The first argument is ignored.
func (d *DynamoMap) StoreIfAbsent(_, val interface{}) (stored bool) {
	valItem, err := MarshalItem(val)
	d.forbidErr(err)
	stored, err2 := d.storeItemIfAbsent(valItem)
	d.forbidErr(err2)
	return stored
}

// LoadOrStore returns the value stored under same key as the given value, if any,
// else stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (d *DynamoMap) loadOrStore(item Item) (Item, bool, error) {
	for {
		if result, loaded, err := d.load(item); loaded || err != nil {
			return result, loaded, err
		}
		if stored, err := d.storeItemIfAbsent(item); stored || err != nil {
			return item, !stored, err
		}
	}
}

func (d *DynamoMap) LoadOrStoreItem(val Itemable) (actual Item, loaded bool, err error) {
	return d.loadOrStore(val.AsItem())
}

func (d *DynamoMap) LoadOrStore(_, val interface{}) (interface{}, bool) {
	valItem, err := MarshalItem(val)
	d.forbidErr(err)
	actual, stored, err2 := d.loadOrStore(valItem)
	d.forbidErr(err2)
	return actual, stored
}

func (d *DynamoMap) storeItemIfVersion(item Item, version int64) (bool, error) {
	hasVersion := expression.Name(d.VersionName).Equal(expression.Value(version))
	err := d.store(item.AsItem(), &hasVersion)
	if ddb.ErrCodeConditionalCheckFailedException == getErrCode(err) {
		return false, nil
	}
	return err == nil, err
}

// StoreItemIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
// Returns true if the item was stored.
func (d *DynamoMap) StoreItemIfVersion(item Itemable, version int64) (ok bool, err error) {
	return d.storeItemIfVersion(item.AsItem(), version)
}

// StoreIfVersion stores the given item if there is an existing item with the same key(s) and the given version.
// Returns true if the item was stored.
func (d *DynamoMap) StoreIfVersion(val interface{}, version int64) (ok bool) {
	valItem, err := MarshalItem(val)
	d.forbidErr(err)
	ok, err2 := d.storeItemIfVersion(valItem, version)
	d.forbidErr(err2)
	return ok
}

func (d *DynamoMap) rangeSegment(ctx context.Context, consumer func(Item) bool, workerId int) error {
	d.debug("starting scan worker: ", workerId)
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
		// check for errors or early termination on other workers
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		// fetch a page
		d.debug("scan request input:", input, ", worker:", workerId)
		resp, err := d.Client.ScanRequest(input).Send()
		d.debug("scan response:", resp, ", worker:", workerId, ", error:", err)
		if err != nil {
			d.debug("scan worker had error:", err, ", worker:", workerId)
			return err
		}
		// run consumer on each record in page
		for _, item := range resp.Items {
			if !consumer(item) {
				d.debug("scan worker received early termination, worker:", workerId)
				return earlyTermination
			}
		}
		// check for next page
		if resp.LastEvaluatedKey == nil {
			d.debug("scan worker done, worker:", workerId)
			return nil
		}
		input.ExclusiveStartKey = resp.LastEvaluatedKey
	}
}

func (d *DynamoMap) RangeItems(consumer func(Item) bool) error {
	// serial
	if d.ScanConcurrency <= 1 {
		return d.rangeSegment(nil, consumer, 0)
	}

	// parallel
	eg, ctx := errgroup.WithContext(context.Background())
	for i := int(0); i < d.ScanConcurrency; i++ {
		workerId := i
		eg.Go(func() error {
			return d.rangeSegment(ctx, consumer, workerId)
		})
	}
	err := eg.Wait()
	if err == earlyTermination {
		return nil
	}
	return err
}

func (d *DynamoMap) Range(consumer func(_, value interface{}) bool) {
	d.RangeItems(func(item Item) bool {
		return consumer(nil, d.unmarshalItem(item))
	})
}
