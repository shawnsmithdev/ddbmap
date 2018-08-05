package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
)

// TableConfig holds details about a specific DynamoDB table, such as its name and key names and types.
type TableConfig struct {
	// The name of the table
	TableName string
	// The name of the hash key attribute.
	HashKeyName string
	// The type of the hash key attribute. Used only if creating a table.
	HashKeyType ddb.ScalarAttributeType
	// The name of the range key attribute, if any.
	RangeKeyName string
	// The type of the range key attribute, if any. Used only if creating a table.
	RangeKeyType ddb.ScalarAttributeType
	// The name of the numeric version field, if any. Used only for those conditional methods that use versions.
	VersionName string
	// The concurrency used in table scans (Range calls). If less than 2, scan is done serially.
	ScanConcurrency int
	// If the client should use strongly consistent reads. This costs twice as much as eventually consistent reads.
	ReadWithStrongConsistency bool
	// CreateTableIfNotExists determines if a table should be created if needed.
	// If true, users must also set the HashKeyType and, if there is a range key, the RangeKeyType, and
	// may choose to also set CreateTableReadCapacity and CreateTableWriteCapacity
	CreateTableIfNotExists bool
	// CreateTableReadCapacity is the read capacity of the new table, if created. 1 is used if less than 1.
	CreateTableReadCapacity int
	// CreateTableReadCapacity is the write capacity of the new table, if created. 1 is used if less than 1.
	CreateTableWriteCapacity int
	// If true, debug logging in this library is enabled.
	Debug bool
	// Logger is the logger used by this library for debug and error logging.
	Logger aws.Logger
	// ValueUnmarshaller can be used to change what is returned by Load, LoadOrStore, and Range.
	// These methods return an Item if ValueUnmarshaller is nil.
	// If ValueUnmarshaller is not nil, the item that would have been returned is given to it,
	// and the results are returned instead.
	ValueUnmarshaller ItemUnmarshaller
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

// NewItemMap creates an ItemMap view of a DynamoDB table from a TableConfig.
// If ScanTableIfNotExists is true and the table does not exist, it will be created.
// If ScanTableIfNotExists is false and the key names are not set, they will be looked up.
// If the logger has not been configured, either the AWS config's logger (if present) or stdout will be used.
func (tc TableConfig) NewItemMap(cfg aws.Config) (ItemMap, error) {
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
	if im.CreateTableIfNotExists {
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
