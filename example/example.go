package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/shawnsmithdev/ddbmap"
	"github.com/shawnsmithdev/ddbmap/ddbconv"
	"math"
)

const greeting = `DynamoDB Map Example Application
This is an example application that both documents how to use ddbmap, and can
act as a test of the correctness of this library's interaction with dynamodb.

To run, this application needs to be configured with credentials to access your
AWS account, such as using envirionmental variables like AWS_PROFILE.
This is similar to configuring the AWS CLI.`

// testRecord is used for the example tool as a typical data structure.
// It has both dynamodbav tags and implements functions to and from Item.
// In the real world only one of these needs to be used.
// Users can add tags and use reflection based marshal/unmarshal options,
// or they can implement Itemable and use the Item based options instead.
type testRecord struct {
	Id       int    `dynamodbav:"id"`
	Name     string `dynamodbav:"name"`
	Friendly bool   `dynamodbav:"friendly"`
	Avatar   []byte `dynamodbav:"avatar"`
	Version  int    `dynamodbav:"version"`
}

func makeTestRecord() testRecord {
	return testRecord{
		Name:     "bob",
		Friendly: true,
		Avatar:   []byte{0xde, 0xad, 0xbe, 0xef},
	}
}

func (tr testRecord) AsItem() ddbmap.Item {
	return ddbmap.Item{
		"id":       ddbconv.ToNumber(tr.Id),
		"name":     ddbconv.ToString(tr.Name),
		"friendly": ddbconv.ToBool(tr.Friendly),
		"avatar":   ddbconv.ToBinary(tr.Avatar),
		"version":  ddbconv.ToNumber(tr.Version),
	}
}

func fromItem(item ddbmap.Item) testRecord {
	return testRecord{
		Id:       item.GetAsNumber("id"),
		Name:     item.GetAsString("name"),
		Friendly: item.GetAsBool("friendly"),
		Avatar:   item.GetAsBinary("avatar"),
		Version:  item.GetAsNumber("version"),
	}
}

func forbidErr(err error) {
	if err != nil {
		panic(err)
	}
}

// TODO: move to config profile
func ddbLocalConfig() aws.Config {
	cfg, err := external.LoadDefaultAWSConfig()
	forbidErr(err)
	cfg.Retryer = aws.DefaultRetryer{
		NumMaxRetries: math.MaxInt32,
	}
	cfg.Credentials = aws.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID:     "foo",
			SecretAccessKey: "bar",
		},
	}
	cfg.EndpointResolver = aws.ResolveWithEndpointURL("http://localhost:8000")
	cfg.Region = endpoints.UsEast1RegionID
	return cfg
}

func main() {
	fmt.Println(greeting)
	tCfg := ddbmap.TableConfig{
		Config:                 ddbLocalConfig(), // TODO
		TableName:              "test",
		HashKeyName:            "id",
		HashKeyType:            dynamodb.ScalarAttributeTypeN,
		CreateTableIfNotExists: true,
		ScanConcurrency:        8,
	}
	itemMap := tCfg.NewItemMap()

	a := makeTestRecord()
	fmt.Printf("\n%+v\n", a)
	itemMap.StoreItem(a)
	b, ok := itemMap.LoadItem(a)
	if !ok {
		panic("not ok)")
	}
	fmt.Printf("%+v\n", fromItem(b))
	itemMap.RangeItems(func(item ddbmap.Item) (getMore bool) {
		fmt.Printf("%+v\n", fromItem(item))
		return true
	})
}
