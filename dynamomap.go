// Package ddbmap presents a map-like interface for DynamoDB tables.
package ddbmap // import "github.com/shawnsmithdev/ddbmap"

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
	"github.com/shawnsmithdev/ddbmap/ddbconv"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

const (
	// How long between checks while waiting for a newly created table to become usable.
	creatingPollDuration = time.Second * 10
	// DefaultTimeToLiveName is used if the TTL duration is set but the ttl attribute name is not.
	DefaultTimeToLiveName = "TTL"
)

var (
	// Indicates that a range operation consumer caused an early termination by returning false. Do not return it.
	errEarlyTermination = fmt.Errorf("ddbmap early termination")

	// interface checks
	_ ItemMap = &DynamoMap{}
	_ Map     = &DynamoMap{}
)

// DynamoMap is a map view of a DynamoDB table.
// The reflection-based api (ddb.Map) will panic on any unhandled AWS client error.
// The Itemable api (ddb.ItemMap) returns errors instead and so will not panic.
type DynamoMap struct {
	TableConfig
	Client *ddb.DynamoDB
}

func (d *DynamoMap) log(vals ...interface{}) {
	if d.Logger == nil {
		log.Println(vals...)
	} else {
		toLog := append([]interface{}{"(ddbmap)"}, vals...)
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

func (d *DynamoMap) unmarshalKey(item Item) interface{} {
	if d.KeyUnmarshaller == nil {
		return nil
	}
	result, err := d.KeyUnmarshaller(d.ToKeyItem(item))
	d.forbidErr(err)
	return result
}

func (d *DynamoMap) unmarshalValue(item Item) interface{} {
	if d.ValueUnmarshaller == nil {
		return item
	}
	result, err := d.ValueUnmarshaller(item)
	d.forbidErr(err)
	return result
}

// DescribeTable checks the table description, returning the table status or any errors.
// If the status is CREATING, the call will poll waiting for the status to change.
// If the table does not exist, the status will be empty.
// If setKeys is true, the keys will be set using the table description.
func (d *DynamoMap) DescribeTable(setKeys bool) (status ddb.TableStatus, err error) {
	input := &ddb.DescribeTableInput{TableName: &d.TableName}
	var dtResp *ddb.DescribeTableOutput

	for {
		d.debug("describe table request input:", input)
		dtReq := d.Client.DescribeTableRequest(input)
		dtResp, err = dtReq.Send()
		d.debug("describe table response:", dtResp, ", error:", err)
		if err != nil {
			if ddb.ErrCodeResourceNotFoundException == getErrCode(err) {
				return "", nil
			}
			return "", err
		}

		status = dtResp.Table.TableStatus
		d.debug("table status:", status)

		switch status {
		case ddb.TableStatusCreating: // Wait for creating
			d.log("waiting for status:", status)
			time.Sleep(creatingPollDuration)
			continue
		case ddb.TableStatusDeleting: // Give up if deleting
			d.log("cannot use table being deleted")
			return status, fmt.Errorf("cannot use table being deleted")
		default: // Table usable, check key names
			if setKeys {
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
			return status, nil
		}
	}
}

// CreateTable creates a new table.
func (d *DynamoMap) CreateTable() error {
	schema := []ddb.KeySchemaElement{
		{AttributeName: &d.HashKeyName, KeyType: ddb.KeyTypeHash},
	}
	attrs := []ddb.AttributeDefinition{
		{AttributeName: &d.HashKeyName, AttributeType: d.HashKeyType},
	}
	if d.Ranged() {
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
		SSESpecification: &ddb.SSESpecification{
			Enabled: aws.Bool(d.ServerSideEncryption),
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

// DeleteItem deletes any existing item with the same key(s) as the given item.
func (d *DynamoMap) DeleteItem(key Itemable) error {
	return d.delete(key.AsItem())
}

// Delete delete the value stored under the same key(s) as the given value, if any.
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

// LoadItem returns the existing item, if present, with the same key(s) as the given item.
// The ok result returns true if the value was found.
func (d *DynamoMap) LoadItem(key Itemable) (item Item, ok bool, err error) {
	return d.load(key.AsItem())
}

// Load returns any value stored under the same key(s) as the given value, if any.
// The ok result indicates if there a value was found for the key.
func (d *DynamoMap) Load(key interface{}) (value interface{}, ok bool) {
	keyItem, err := MarshalItem(key)
	d.forbidErr(err)
	resultItem, ok, err := d.load(keyItem)
	d.forbidErr(err)
	return d.unmarshalValue(resultItem), ok
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
	if d.TimeToLiveDuration > 0 {
		ttl := ddbconv.EncodeInt(int(time.Now().Add(d.TimeToLiveDuration).Unix()))
		if "" == d.TimeToLiveName {
			input.Item[DefaultTimeToLiveName] = ttl
		} else {
			input.Item[d.TimeToLiveName] = ttl
		}
	}
	d.debug("store request input:", input)
	resp, err := d.Client.PutItemRequest(input).Send()
	d.debug("store response:", resp, ", error:", err)
	return err
}

// StoreItem stores the given item, clobbering any existing item with the same key(s).
func (d *DynamoMap) StoreItem(val Itemable) error {
	return d.store(val.AsItem(), nil)
}

// Store stores the given value. The first argument is ignored.
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

// LoadOrStore returns the value stored under same key(s) as the given value, if any,
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

// LoadOrStoreItem returns the existing item, if present, with the same key(s) as the given item.
// Otherwise, it stores and returns the given item.
// The loaded result is true if the value was loaded, false if stored.
func (d *DynamoMap) LoadOrStoreItem(val Itemable) (actual Item, loaded bool, err error) {
	return d.loadOrStore(val.AsItem())
}

// LoadOrStore returns any value stored that has the same key as the given value, if any,
// else stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
// The first argument is ignored.
func (d *DynamoMap) LoadOrStore(_, val interface{}) (interface{}, bool) {
	valItem, err := MarshalItem(val)
	d.forbidErr(err)
	actual, stored, err := d.loadOrStore(valItem)
	d.forbidErr(err)
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

// RangeItems calls the given consumer for each stored item.
// Iteration eventually stops if the given function returns false.
func (d *DynamoMap) RangeItems(consumer func(Item) bool) error {
	input := ddb.ScanInput{
		TableName:      &d.TableName,
		ConsistentRead: &d.ReadWithStrongConsistency,
		Select:         ddb.SelectAllAttributes,
	}
	worker := scanWorker{
		input:    &input,
		table:    d,
		consumer: consumer,
	}

	if d.ScanConcurrency <= 1 {
		return worker.work()
	}

	group, ctx := errgroup.WithContext(context.Background())
	input.TotalSegments = aws.Int64(int64(d.ScanConcurrency))
	worker.ctx = ctx
	for i := 0; i < d.ScanConcurrency; i++ {
		group.Go(worker.withId(i, input).work)
	}
	err := group.Wait()
	if err == errEarlyTermination {
		return nil
	}
	return err
}

// Range iterates over the map and applies the given function to every value.
// Iteration eventually stops if the given function returns false.
// The consumed key will be nil unless KeyUnmarshaller is set.
// The consumed value will be an Item unless ValueUnmarshaller is set.
func (d *DynamoMap) Range(consumer func(key, value interface{}) bool) {
	d.forbidErr(d.RangeItems(func(item Item) bool {
		return consumer(d.unmarshalKey(item), d.unmarshalValue(item))
	}))
}
