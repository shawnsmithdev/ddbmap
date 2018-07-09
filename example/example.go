package main

import (
	"fmt"
	"github.com/shawnsmithdev/ddbmap"
	"github.com/shawnsmithdev/ddbmap/ddbconv"
	"testing"
)

const greeting = `DynamoDB Map Example Application
This is an example application that both documents how to use ddbmap, and can
act as a test of the correctness of this library's interaction with dynamodb.

To run, this application needs to be configured with credentials to access your
AWS account, such as using envirionmental variables like AWS_PROFILE.
This is similar to configuring the AWS CLI.
`

// testRecord is used for the example tool as a typical data structure.
// It has both dynamodbav tags and implements functions to and from Item.
// In the real world only one of these needs to be used.
// Users can add tags and use reflection based marshal/unmarshal options,
// or they can implement Itemable and use the Item based options instead.
type testRecord struct {
	Id       int    `dynamodbav:"id"`
	Name     string `dynamodbav:"name"`
	Age      int    `dynamodbav:"age"`
	Friendly bool   `dynamodbav:"friendly"`
	Avatar   []byte `dynamodbav:"avatar"`
}

func makeTestRecord() testRecord {
	return testRecord{
		Id:       1,
		Name:     "bob",
		Age:      40,
		Friendly: true,
		Avatar:   []byte{0xde, 0xad, 0xbe, 0xef},
	}
}

func (tr testRecord) AsItem() ddbmap.Item {
	return ddbmap.Item{
		"id":       ddbconv.ToNumber(tr.Id),
		"name":     ddbconv.ToString(tr.Name),
		"age":      ddbconv.ToNumber(tr.Age),
		"friendly": ddbconv.ToBool(tr.Friendly),
		"avatar":   ddbconv.ToBinary(tr.Avatar),
	}
}

func fromItem(item ddbmap.Item) testRecord {
	return testRecord{
		Id:       item.GetAsNumber("id"),
		Name:     item.GetAsString("name"),
		Age:      item.GetAsNumber("age"),
		Friendly: item.GetAsBool("friendly"),
		Avatar:   item.GetAsBinary("avatar"),
	}
}

func testItemMap(m ddbmap.ItemMap, t *testing.T) {
	x := makeTestRecord()
	m.StoreItem(x)
	if loaded, loadOk := m.LoadItem(x); loadOk {
		y := fromItem(loaded)
		if x.Name != y.Name {
			t.Fail()
		}
	} else {
		t.Fail()
	}
	m.DeleteItem(x)
	if _, loadOk := m.LoadItem(x); loadOk {
		t.Fail()
	}
}

func testMap(m ddbmap.Map, t *testing.T) {
	x := makeTestRecord()
	m.Store(x.Id, x)
	if loaded, loadOk := m.Load(x.Id); loadOk {
		if y, castOk := loaded.(testRecord); castOk {
			if x.Name != y.Name {
				t.Fail()
			}
		} else {
			t.Fail()
		}
	} else {
		t.Fail()
	}
	m.Delete(x.Id)
	if _, loadOk := m.Load(x.Id); loadOk {
		t.Fail()
	}
}

func main() {
	fmt.Print(greeting)
}
