package ddbmap

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"os"
	"time"
)

// CreateTableOptions contain values used when creating new DynamoDB tables
type CreateTableOptions struct {
	// CreateTableIfAbsent determines if a table should be created when missing.
	// If true, users must also set the HashKeyType and, if there is a range key, the RangeKeyType,
	// and may also choose to set CreateTableReadCapacity and CreateTableWriteCapacity
	CreateTableIfAbsent bool
	// CreateTableReadCapacity is the read capacity of the new table, if created. 1 is used if less than 1.
	CreateTableReadCapacity int
	// CreateTableReadCapacity is the write capacity of the new table, if created. 1 is used if less than 1.
	CreateTableWriteCapacity int
	// The type of the hash key attribute.
	HashKeyType ddb.ScalarAttributeType
	// The type of the range key attribute, if any.
	RangeKeyType ddb.ScalarAttributeType
	// If true, Server Side Encryption (SSE) is enabled.
	ServerSideEncryption bool
}

// TableConfig holds details about a specific DynamoDB table and some options for using it.
type TableConfig struct {
	// The name of the table.
	TableName string
	// The name of the hash key attribute.
	HashKeyName string
	// The name of the range key attribute, if any.
	RangeKeyName string
	// The name of the numeric version field, if any.
	// Used only for those conditional methods that use versions.
	VersionName string
	// The name of the ttl field, if any.
	// If empty and TimeToLiveDuration is not zero, DefaultTimeToLiveName ("TTL") will be used.
	// A ttl field should be either an int type or dynamodbattribute.UnixTime.
	TimeToLiveName string
	// The Time To Live Duration, if any.
	TimeToLiveDuration time.Duration
	// The concurrency used in table scans (Range calls).
	// If less than 2, scan is done serially.
	ScanConcurrency int
	// If the client should use strongly consistent reads.
	// This costs twice as much as eventually consistent reads.
	ReadWithStrongConsistency bool
	// If true, debug logging in this library is enabled.
	Debug bool
	// Logger is the logger used by this library for debug and error logging.
	Logger aws.Logger
	// ValueUnmarshaller can be used to change what is returned by Load, LoadOrStore, and Range.
	// These methods return an Item if ValueUnmarshaller is nil.
	// If ValueUnmarshaller is not nil, the result of passing the value item to the unmarshaller
	// is returned as the value instead of the item.
	ValueUnmarshaller ItemUnmarshaller
	// Options for creating the table
	CreateTableOptions
}

// Ranged returns true if RangeKeyName is not empty
func (tc TableConfig) Ranged() bool {
	return len(tc.RangeKeyName) > 0
}

// ToKeyItem returns an item with only the configured key(s) copied from the given item.
func (tc TableConfig) ToKeyItem(item Item) Item {
	if tc.Ranged() {
		return item.Project(tc.HashKeyName, tc.RangeKeyName)
	}
	return item.Project(tc.HashKeyName)
}

// NewMap creates a map view of a DynamoDB table from a TableConfig.
// If the table does not exist or is being deleted or there is an error, the pointer result will be nil.
// If ScanTableIfNotExists is true and the table does not exist, it will be created.
// If ScanTableIfNotExists is false and the key names are not set, they will be looked up.
// If the logger has not been configured, either the AWS config's logger (if present) or stdout will be used.
func (tc TableConfig) NewMap(cfg aws.Config) (*DynamoMap, error) {
	if tc.Logger == nil {
		if cfg.Logger == nil {
			tc.Logger = logTo(os.Stdout)
		} else {
			tc.Logger = cfg.Logger
		}
	}
	im := &DynamoMap{
		TableConfig: tc,
		Client:      ddb.New(cfg),
	}
	var status ddb.TableStatus
	err := error(nil)

	if tc.CreateTableIfAbsent {
		status, err = im.DescribeTable(false)
		if "" == status {
			err = im.CreateTable()
		}
	} else if "" == tc.HashKeyName {
		status, err = im.DescribeTable(true)
		if "" == status {
			return nil, errors.New("table does not exist, and hash key name is empty")
		}
	}
	if err != nil {
		return nil, err
	}
	return im, nil
}
