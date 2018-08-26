[![Build Status](https://travis-ci.org/shawnsmithdev/ddbmap.svg)](https://travis-ci.org/shawnsmithdev/ddbmap)
[![GoDoc](https://godoc.org/github.com/shawnsmithdev/ddbmap?status.png)](https://godoc.org/github.com/shawnsmithdev/ddbmap)
[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/shawnsmithdev/ddbmap/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/shawnsmithdev/ddbmap)](https://goreportcard.com/report/github.com/shawnsmithdev/ddbmap)

# ddbmap
`ddbmap` is a Go (golang) library and module that presents a map-like view of an AWS DynamoDB table.

# Example
```go
package main

import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/aws/external"
    "github.com/shawnsmithdev/ddbmap"
    "fmt"
)

type Person struct {
    Id   int
    Name string
    Age  int
}

func main() {
    awsCfg, _ := external.LoadDefaultAWSConfig()
    awsCfg.Retryer = aws.DefaultRetryer{NumMaxRetries: 100}

    // Assumes table already exists, will auto-discover key names
    tCfg := ddbmap.TableConfig{
        TableName:         "TestTable",
        ValueUnmarshaller: ddbmap.UnmarshallerForType(Person{}),
    }
    people, _ := tCfg.NewMap(awsCfg)

    // put
    p1 := Person{Id: 1, Name: "Bob", Age: 20}
    err := people.Store(p1)

    // get
    p2, loaded, err := people.Load(Person{Id: 1})
    if loaded && err == nil {
        fmt.Println(p2.(Person)) // same as p1
    }

    // iterate
    err = people.Range(func(p3 interface{}) bool {
        fmt.Println(p3.(Person)) // same as p1
        return true
    })
}
```

# Your table, as a map
One way to view a DynamoDB table is as kind of a [hashmap](https://en.wikipedia.org/wiki/Hash_table) in the cloud.

This library ignores some of the features of DynamoDB, such as range key queries and batching,
to provide a simple API to access a table.

* Get a single record
* Put a single record
* Delete a single record
* Conditional Put If Absent
* Iterate over all records (serially or in parallel)

Note that you must either use capitalized DynamoDB field names, or add struct tags like `dynamodbav` to rename
exported fields.

# Item API
The `ddbmap.ItemMap` API may be used by implementing `ddbmap.Itemable` to handle conversions between the Go and
DynamoDB type system, with or without using reflection.

# Conditional Updates (versions)
[Conditional updates](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate),
where the condition is stronger than just a record's absence, is supported by defining a numerical version field
and configuring `VersionName` in the `TableConfig` to the name of that field.

# Dependencies
This library depends on the AWS Go SDK v2 and `golang.org/x/sync`.
If building with a go version older than 1.11, you will need to install these dependencies manually.
```
go get -u github.com/aws/aws-sdk-go-v2
go get -u golang.org/x/sync
```

# Conditional Updates (versions)
Using the Item API, if users wishes to do
[conditional updates](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate),
where the condition is stronger than just a record's presence or absence, they should define a numerical version field
and configure `VersionName` in their `TableConfig` to the name of that field. Dynamo can support conditional operations
on any field, but the potential for losing updates is too high if update conditions depend on fields that do not
obviously need to be changed on update. An explicit version field can help avoid an entire class of potential concurrent
modification bugs, so that is all this library supports.

# TODO
* Test range early termination
* Test other set types, null
