// +build integration

package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

const (
	endpointEnv       = "DDBMAP_INTEG_ENDPOINT"
	debugEnv          = "DDBMAP_INTEG_DEBUG"
	testTableName     = "TestTable"
	hashKeyName       = "Id"
	retries           = 16
	testTTL           = 2 * time.Hour
	testMaxElapsedTTL = time.Minute
)

// Users can choose to define a distinct hashable struct type for keys, to better simulate a hashmap.
type personKey struct {
	Id int
}

// Testing and example data structure
type person struct {
	personKey
	Name string
	Age  int
	// Defining the ttl field in your struct is not required to use the time to live feature.
	TTL dynamodbattribute.UnixTime
}

func checkMap(people Map, t *testing.T) {
	// put
	p1 := person{personKey: personKey{Id: 1}, Name: "Bob", Age: 20}
	people.Store(p1.personKey, p1)

	// get
	p2, ok := people.Load(p1.personKey)
	if !ok {
		t.Fatal("expected value from get doesn't exist")
	}

	// If a dynamodb map, test ttl and save in expected value
	if _, ok := people.(*DynamoMap); ok {
		ttl := p2.(person).TTL
		elapsed := testTTL - time.Time(ttl).Sub(time.Now())
		// some small amount of ttl time should have elapsed
		if elapsed < 0 || elapsed > testMaxElapsedTTL {
			t.Fatal("remaining ttl elapsed:", elapsed)
		}
		p1.TTL = ttl
	}
	// compare everything else
	if !reflect.DeepEqual(p2, p1) {
		t.Fatal("expected value from get doesn't match")
	}

	// iterate
	exists := false
	match := false
	people.Range(func(key, val interface{}) bool {
		exists = true
		match = reflect.DeepEqual(key.(personKey), p1.personKey)
		match = match && reflect.DeepEqual(val.(person), p1)
		return true
	})
	if !exists {
		t.Fatal("expected value from scan doesn't exist")
	}
	if !match {
		t.Fatal("expected value from scan doesn't match")
	}
}

type testEnv struct {
	debug    bool
	endpoint string
}

func (e testEnv) useEndpoint(cfg *aws.Config) {
	cfg.EndpointResolver = aws.ResolveWithEndpointURL(e.endpoint)
}

func getTestEnv(t *testing.T) testEnv {
	var result testEnv
	if endpoint, ok := os.LookupEnv(endpointEnv); ok && endpoint != "" {
		t.Log("endpoint:", endpoint)
		result.endpoint = endpoint
	}
	_, result.debug = os.LookupEnv(debugEnv)
	if result.debug {
		t.Log("debug enabled")
	}
	return result
}

func TestSyncMap(t *testing.T) {
	var people sync.Map
	checkMap(&people, t)
}

func TestDynamoMap(t *testing.T) {
	awsCfg, _ := external.LoadDefaultAWSConfig()
	awsCfg.Retryer = aws.DefaultRetryer{NumMaxRetries: retries}
	env := getTestEnv(t)
	env.useEndpoint(&awsCfg)

	tCfg := TableConfig{
		TableName:          testTableName,
		HashKeyName:        hashKeyName,
		Debug:              env.debug,
		TimeToLiveDuration: testTTL,
		KeyUnmarshaller:    UnmarshallerForType(personKey{}),
		ValueUnmarshaller:  UnmarshallerForType(person{}),
		CreateTableOptions: CreateTableOptions{
			CreateTableIfAbsent: true,
			HashKeyType:         dynamodb.ScalarAttributeTypeN,
		},
	}
	people, err := tCfg.NewMap(awsCfg)
	if err != nil {
		t.Fatal(err)
	}
	checkMap(people, t)
}
