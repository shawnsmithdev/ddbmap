package ddbmap

import "github.com/aws/aws-sdk-go-v2/service/dynamodb"

type testRecord struct {
	Id       int64
	Name     string
	Age      int
	Friendly bool
}

func (tr testRecord) AsItem() Item {
	return map[string]dynamodb.AttributeValue{
		"id":       Int64ToN(tr.Id),
		"name":     StringToS(tr.Name),
		"age":      IntToN(tr.Age),
		"friendly": BoolToBOOL(tr.Friendly),
	}
}

func fromItem(item Item) testRecord {
	return testRecord{
		Id:       item.GetInt64("id"),
		Name:     item.GetString("name"),
		Age:      item.GetInt("age"),
		Friendly: item.GetBool("friendly"),
	}
}
