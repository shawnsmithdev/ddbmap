package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type TableConfig struct {
	// The AWS configuration to use.
	// Changing the aws config after creating the table config may result in undefined behavior, so don't.
	aws.Config
	// The name of the table
	TableName string
	// The name of the hash key attribute
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
	ScanConcurrency int64
	// If the client should use strongly consistent reads. This costs twice as much as eventually consistent reads.
	ReadWithStrongConsistency bool
	// CreateTableIfNotExists determines if this client will create the table if needed.
	// If true, users must also set the HashKeyType and, if there is a range key, the RangeKeyType.
	CreateTableIfNotExists bool
	// CreateTableReadCapacity is the read capacity of the new table, if created. 1 is used if less than 1.
	CreateTableReadCapacity int64
	// CreateTableReadCapacity is the write capacity of the new table, if created. 1 is used if less than 1.
	CreateTableWriteCapacity int64
}

func (tc TableConfig) ranged() bool {
	return len(tc.RangeKeyName) > 0
}

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

func (tc TableConfig) NewItemMap() ItemMap {
	im := &ddbmap{
		TableConfig: tc,
		svc:         ddb.New(tc.Config),
	}
	im.checkExists()
	return im
}
