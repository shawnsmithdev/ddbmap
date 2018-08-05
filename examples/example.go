package main

import (
	"flag"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/shawnsmithdev/ddbmap"
	"github.com/shawnsmithdev/ddbmap/ddbconv"
	"log"
	"os"
	"sync"
)

const greeting = `DynamoDB Map (ddbmap) Example Application
This is an example application that both documents how to use ddbmap, and can
act as a test of the correctness of this library's interaction with dynamodb.

To run, this application needs to be configured with credentials to access your
AWS account, such as using envirionmental variables like AWS_PROFILE.
This is similar to configuring the AWS CLI.

You can use DynamoDB Local with the "endpoint" cli flag and a profile with fake credentials and region.
example: AWS_PROFILE=local ./example -endpoint=http://localhost:8000

You can turn on library debug logging with '-v', or AWS SDK debug logging with '-awsv'.
`

// userKey is a hashable type to store key for a user.
type userKey struct {
	Id int
}

func (uk userKey) AsItem() ddbmap.Item {
	return ddbmap.Item{
		userIdField: ddbconv.ToInt(uk.Id),
	}
}

// user is a typical data structure
type user struct {
	userKey
	Name     string
	Friendly bool
	Avatar   []byte
	Version  int
}

const (
	userTableName = "test-user"

	userIdField       = "id"
	userNameField     = "name"
	userFriendlyField = "friendly"
	userAvatarField   = "avatar"
	userVersionField  = "version"
)

func (tr user) AsItem() ddbmap.Item {
	return ddbmap.Item{
		userIdField:       ddbconv.ToInt(tr.Id),
		userNameField:     ddbconv.ToString(tr.Name),
		userFriendlyField: ddbconv.ToBool(tr.Friendly),
		userAvatarField:   ddbconv.ToBinary(tr.Avatar),
		userVersionField:  ddbconv.ToInt(tr.Version),
	}
}

func userFromItem(item ddbmap.Item) user {
	return user{
		userKey:  userKey{Id: item.GetAsInt(userIdField)},
		Name:     item.GetAsString(userNameField),
		Friendly: item.GetAsBool(userFriendlyField),
		Avatar:   item.GetAsBinary(userAvatarField),
		Version:  item.GetAsInt(userVersionField),
	}
}

func getUserDynamo(libDebug bool) ddbmap.TableConfig {
	return ddbmap.TableConfig{
		TableName:                 userTableName,
		HashKeyName:               userIdField,
		HashKeyType:               dynamodb.ScalarAttributeTypeN,
		CreateTableIfNotExists:    true,
		ScanConcurrency:           8,
		ReadWithStrongConsistency: true,
		VersionName:               userVersionField,
		Debug:                     libDebug,
	}
}

var bob = user{
	userKey:  userKey{Id: 4},
	Name:     "bob",
	Friendly: true,
	Avatar:   []byte{0xde, 0xad, 0xbe, 0xef},
}

func checkEqualBob(b user) {
	if b.Id != bob.Id || b.Name != bob.Name || b.Avatar[2] != bob.Avatar[2] {
		log.Panicf("%+v not equal to %+v", b, bob)
	}
}

// Example of using the interface-based map methods.
func testUser(itemMap ddbmap.ItemMap) {
	// Test storing a user
	itemMap.StoreItem(bob)

	// Test loading a user
	bItem, ok, err := itemMap.LoadItem(bob.userKey)
	if err != nil {
		panic(err)
	}
	if !ok {
		panic("not ok")
	}
	checkEqualBob(userFromItem(bItem))

	// Test ranging across all stored items
	itemMap.RangeItems(func(item ddbmap.Item) (getMore bool) {
		checkEqualBob(userFromItem(item))
		return true
	})
}

// game is a typical data structure that uses reflection and struct tags.
type gameKey struct {
	Id int `dynamodbav:"id"`
}

type game struct {
	gameKey
	Name     string `dynamodbav:"name"`
	Mature   bool   `dynamodbav:"mature"`
	CoverArt []byte `dynamodbav:"cover_art"`
	Version  int    `dynamodbav:"version"`
}

const (
	gameId        = "id"
	gameTableName = "test-game"
)

func testGameMap(itemMap ddbmap.Map) {
	// Test storing a user
	a := game{
		gameKey:  gameKey{Id: 4},
		Name:     "bob's game",
		Mature:   false,
		CoverArt: []byte{0xde, 0xad, 0xbe, 0xef},
		Version:  0,
	}
	itemMap.Store(a.gameKey, a)

	// Test loading a user as a game pointer, since we set the ValueSupplier
	// Without ValueSupplier, they'd be Item and we'd need to demarshal ourselves
	val, ok := itemMap.Load(a.gameKey)
	if !ok {
		panic("not ok")
	}
	if val.(game).Name != "bob's game" {
		panic("not bob's game")
	}

	_, missingOk := itemMap.Load(gameKey{Id: 42})
	if missingOk {
		panic("ok when missing")
	}
	// Test ranging across all stored items
	itemMap.Range(func(_, val interface{}) (getMore bool) {
		if val.(game).Name != "bob's game" {
			panic("not bob's game")
		}
		return true
	})
}

// Example of using the reflection-based map methods.
func buildDynamoTestGameConfig(libDebug bool) ddbmap.TableConfig {
	return ddbmap.TableConfig{
		TableName:                 gameTableName,
		HashKeyName:               gameId,
		HashKeyType:               dynamodb.ScalarAttributeTypeN,
		CreateTableIfNotExists:    true,
		ReadWithStrongConsistency: true,
		Debug:                     libDebug,
		ValueUnmarshaller:         ddbmap.UnmarshallerForType(game{}),
	}
}

func checkFlags(cfg aws.Config) (aws.Config, bool) {
	var endpoint string
	var awsVerbose, libVerbose bool
	flag.StringVar(&endpoint, "endpoint", "",
		"Optional static endpoint URL, ex. http://localhost:8000")
	flag.BoolVar(&awsVerbose, "awsv", false,
		"If true, awsVerbose AWS debug logging with HTTP body is enabled")
	flag.BoolVar(&libVerbose, "v", false,
		"If true, ddbmap library debug logging is enabled")
	flag.Parse()
	if "" != endpoint {
		log.Println("Using endpoint:", endpoint)
		cfg.EndpointResolver = aws.ResolveWithEndpointURL(endpoint)
	}
	if awsVerbose {
		cfg.LogLevel |= aws.LogDebugWithHTTPBody
	}
	return cfg, libVerbose
}

func main() {
	log.SetOutput(os.Stdout)
	log.Print(greeting)

	// aws config
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic(err)
	}
	cfg.Retryer = aws.DefaultRetryer{NumMaxRetries: 1 << 30}
	var libDebug bool
	cfg, libDebug = checkFlags(cfg)

	// Test reflection API using Dynamo
	log.Println("start test reflection dynamo")
	tCfg := buildDynamoTestGameConfig(libDebug)
	var tableMap ddbmap.Map
	tableMap, err = tCfg.NewItemMap(cfg)
	if err != nil {
		panic(err)
	}
	testGameMap(tableMap)
	log.Println("end test reflection dynamo")

	// Test reflection API using sync.Map
	log.Println("start test reflection sync.Map")
	var gm sync.Map
	tableMap = &gm
	testGameMap(tableMap)
	log.Println("end test reflection sync.Map")

	// Test Itemable API using Dynamo
	log.Println("start test itemable dynamo")
	var table ddbmap.ItemMap
	tCfg = getUserDynamo(libDebug)
	table, err = tCfg.NewItemMap(cfg)
	if err != nil {
		panic(err)
	}
	testUser(table)
	log.Println("end test Itemable dynamo")

	// Test key discovery
	log.Println("start test key discovery dynamo")
	tCfg.CreateTableIfNotExists = false
	tCfg.HashKeyName = ""
	tCfg.HashKeyType = ""
	table, err = tCfg.NewItemMap(cfg)
	if err != nil {
		panic(err)
	}
	testUser(table)
	log.Println("end test key discovery dynamo")
}
