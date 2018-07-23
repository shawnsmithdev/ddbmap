package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/shawnsmithdev/ddbmap"
	"github.com/shawnsmithdev/ddbmap/ddbconv"
)

const greeting = `DynamoDB Map (ddbmap) Example Application
This is an example application that both documents how to use ddbmap, and can
act as a test of the correctness of this library's interaction with dynamodb.

To run, this application needs to be configured with credentials to access your
AWS account, such as using envirionmental variables like AWS_PROFILE.
This is similar to configuring the AWS CLI.

You can use DynamoDB Local with the "endpoint" cli flag and a profile with fake credentials and region.
example: AWS_PROFILE=local ./example -endpoint=http://localhost:8000
`

// userKey is a hashable type to store key for a user.
type userKey struct {
	Id int
}

func (uk userKey) AsItem() ddbmap.Item {
	return ddbmap.Item{
		userIdField: ddbconv.ToNumber(uk.Id),
	}
}

func userKeyFromItem(item ddbmap.Item) interface{} {
	return userKey{Id: ddbconv.FromNumber(item[userIdField])}
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
		userIdField:       ddbconv.ToNumber(tr.Id),
		userNameField:     ddbconv.ToString(tr.Name),
		userFriendlyField: ddbconv.ToBool(tr.Friendly),
		userAvatarField:   ddbconv.ToBinary(tr.Avatar),
		userVersionField:  ddbconv.ToNumber(tr.Version),
	}
}

func userFromItem(item ddbmap.Item) user {
	return user{
		userKey:  userKey{Id: item.GetAsNumber(userIdField)},
		Name:     item.GetAsString(userNameField),
		Friendly: item.GetAsBool(userFriendlyField),
		Avatar:   item.GetAsBinary(userAvatarField),
		Version:  item.GetAsNumber(userVersionField),
	}
}

func getUserDynamo(cfg aws.Config) ddbmap.TableConfig {
	return ddbmap.TableConfig{
		AWSConfig:                 cfg,
		TableName:                 userTableName,
		HashKeyName:               userIdField,
		HashKeyType:               dynamodb.ScalarAttributeTypeN,
		CreateTableIfNotExists:    true,
		ScanConcurrency:           8,
		ReadWithStrongConsistency: true,
		VersionName:               userVersionField,
	}
}

// Example of using the interface-based map methods.
func testUser(itemMap ddbmap.ItemMap) {

	// Test storing a user
	a := user{
		userKey:  userKey{Id: 4},
		Name:     "bob",
		Friendly: true,
		Avatar:   []byte{0xde, 0xad, 0xbe, 0xef},
	}
	fmt.Println(a)
	itemMap.StoreItem(a)

	// Test loading a user
	b, ok, err := itemMap.LoadItem(userKey{Id: 4})
	if err != nil {
		panic(err)
	}
	if !ok {
		panic("not ok")
	}
	fmt.Println(userFromItem(b))

	// Test ranging across all stored items
	itemMap.RangeItems(func(item ddbmap.Item) (getMore bool) {
		fmt.Println(userFromItem(item))
		return true
	})
}

// game is a typical data structure that uses reflection and struct tags.
type game struct {
	Id       int    `dynamodbav:"id"`
	Name     string `dynamodbav:"name"`
	Mature   bool   `dynamodbav:"mature"`
	CoverArt []byte `dynamodbav:"cover_art"`
	Version  int    `dynamodbav:"version"`
}

const (
	gameId        = "id"
	gameTableName = "test-game"
)

func gameFromItem(item interface{}) game {
	var b game
	ddbmap.UnmarshalItem(item.(ddbmap.Item), &b)
	return b
}

// Example of using the reflection-based map methods.
func testGame(cfg aws.Config) {
	// Configure the map
	tCfg := ddbmap.TableConfig{
		AWSConfig:                 cfg,
		TableName:                 gameTableName,
		HashKeyName:               gameId,
		HashKeyType:               dynamodb.ScalarAttributeTypeN,
		CreateTableIfNotExists:    true,
		ReadWithStrongConsistency: true,
	}

	// Create the map
	var itemMap ddbmap.Map = tCfg.NewItemMap()

	// Test storing a user
	a := game{
		Id:       4,
		Name:     "bob's game",
		Mature:   false,
		CoverArt: []byte{0xde, 0xad, 0xbe, 0xef},
		Version:  0,
	}
	fmt.Println(a)
	itemMap.Store(a, a)

	// Test loading a user
	item, ok := itemMap.Load(a)
	if !ok {
		panic("not ok")
	}
	fmt.Println(gameFromItem(item))

	// Test ranging across all stored items
	itemMap.Range(func(_, value interface{}) (getMore bool) {
		fmt.Println(gameFromItem(value))
		return true
	})
}

func checkFlags(cfg aws.Config) aws.Config {
	var endpoint string
	var verbose bool
	flag.StringVar(&endpoint, "endpoint", "",
		"Optional static endpoint URL, ex. http://localhost:8000")
	flag.BoolVar(&verbose, "v", false,
		"If true, verbose debug logging with HTTP body is enabled")
	flag.Parse()
	if "" != endpoint {
		fmt.Println("Using endpoint:", endpoint)
		cfg.EndpointResolver = aws.ResolveWithEndpointURL(endpoint)
	}
	if verbose {
		cfg.LogLevel |= aws.LogDebugWithHTTPBody
	}
	return cfg
}

func main() {
	fmt.Print(greeting)
	fmt.Println()

	// aws config
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic(err)
	}
	cfg = checkFlags(cfg)

	// Test Itemable API using Dynamo
	table := getUserDynamo(cfg)
	table.Debug = true
	testUser(table.NewItemMap())

	// Test key discovery
	table.Debug = true
	table.CreateTableIfNotExists = false
	table.HashKeyName = ""
	table.HashKeyType = ""
	testUser(table.NewItemMap())

	// Test Itemable API using sync.Map
	testUser(&ddbmap.SyncItemMap{
		Keyer: userKeyFromItem,
	})

	// Test reflection API using Dynamo
	testGame(cfg)
}
