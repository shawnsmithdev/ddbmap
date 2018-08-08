package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
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
}

// TableConfig holds details about a specific DynamoDB table and some options for using it.
type TableConfig struct {
	// The name of the table.
	TableName string
	// The name of the hash key attribute.
	HashKeyName string
	// The name of the range key attribute, if any.
	RangeKeyName string
	// The name of the numeric version field, if any. Used only for those conditional methods that use versions.
	VersionName string
	// The concurrency used in table scans (Range calls). If less than 2, scan is done serially.
	ScanConcurrency int
	// If the client should use strongly consistent reads. This costs twice as much as eventually consistent reads.
	ReadWithStrongConsistency bool
	// If true, debug logging in this library is enabled.
	Debug bool
	// Logger is the logger used by this library for debug and error logging.
	Logger aws.Logger
	// ValueUnmarshaller can be used to change what is returned by Load, LoadOrStore, and Range.
	// These methods return an Item if ValueUnmarshaller is nil.
	// If ValueUnmarshaller is not nil, the result of passing the item to the unmarshaller is returned instead.
	ValueUnmarshaller ItemUnmarshaller
	// Options for creating tables
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

// NewMap creates an map view of a DynamoDB table from a TableConfig.
// If ScanTableIfNotExists is true and the table does not exist, it will be created.
// If ScanTableIfNotExists is false and the key names are not set, they will be looked up.
// If the logger has not been configured, either the AWS config's logger (if present) or stdout will be used.
func (tc TableConfig) NewMap(cfg aws.Config) (*DynamoMap, error) {
	if tc.Logger == nil {
		if cfg.Logger == nil {
			tc.Logger = aws.LoggerFunc(log.Println)
		} else {
			tc.Logger = cfg.Logger
		}
	}
	im := &DynamoMap{
		TableConfig: tc,
		Client:      ddb.New(cfg),
	}
	if im.CreateTableIfAbsent {
		if ok, err := im.describeTable(false); !ok {
			if err != nil {
				return nil, err
			}
			if err = im.createTable(); err != nil {
				return nil, err
			}
		}
	} else if "" == im.HashKeyName {
		im.describeTable(true)
	}
	return im, nil
}
