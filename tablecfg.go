package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// TableConfig holds details about a specific DynamoDB table, such as its name and key names and types.
type TableConfig struct {
	// The AWS configuration to use.
	// Changing the aws config after creating the table config may result in undefined behavior, so don't.
	AWSConfig aws.Config
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
	// The name of the version field, if any. Used only if making conditional calls.
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
	// If true, debug logging in this library is enabled
	Debug bool
	// LoggerFunc is the logger used by this library for debug logging. The AWS config logger is used if nil.
	Logger aws.Logger
}

func (tc TableConfig) ranged() bool {
	return len(tc.RangeKeyName) > 0
}

// ToKeyItem returns an item with only the configured key(s) copied from the given item.
// TODO: move somewhere else
func (tc TableConfig) ToKeyItem(item Item) (result Item) {
	if len(item) == 1 || (tc.ranged() && len(item) == 2) {
		result = item
	} else {
		result = Item{tc.HashKeyName: item[tc.HashKeyName]}
		if tc.ranged() {
			result[tc.RangeKeyName] = item[tc.RangeKeyName]
		}
	}
	return result
}

// NewItemMap creates an ItemMap view of a DynamoDB table from a TableConfig.
// If ScanTableIfNotExists is true and the table does not exist, it will be created.
func (tc TableConfig) NewItemMap() ItemMap {
	im := &ddbmap{
		TableConfig: tc,
		svc:         ddb.New(tc.AWSConfig),
	}
	if im.CreateTableIfNotExists {
		if !im.describeTable(false) {
			im.createTable()
		}
	} else if "" == im.HashKeyName {
		im.describeTable(true)
	}
	return im
}
