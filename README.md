[![Build Status](https://travis-ci.org/shawnsmithdev/ddbmap.svg)](https://travis-ci.org/shawnsmithdev/ddbmap)
[![GoDoc](https://godoc.org/github.com/shawnsmithdev/ddbmap?status.png)](https://godoc.org/github.com/shawnsmithdev/ddbmap)
[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/shawnsmithdev/ddbmap/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/shawnsmithdev/ddbmap)](https://goreportcard.com/report/github.com/shawnsmithdev/ddbmap)

# ddbmap
`ddbmap` is a Go (golang) library and module that presents a map-like view of an AWS DynamoDB table.

This library depends on the AWS Go SDK v2 and `golang.org/x/sync`.
If building with a go version older than 1.11, you will need to install these dependencies manually.
```
go get -u github.com/aws/aws-sdk-go-v2
go get -u golang.org/x/sync
```

The current tagged version is v0.0.x, which [implies](https://tip.golang.org/cmd/go/#hdr-Module_compatibility_and_semantic_versioning) that users should have *no expectations of stability or backwards compatibility*. This is especially true while AWS Go SDK v2 is still in unreleased preview.

# Motivation
The AWS Go SDK is fairly low level. It acts as a kind of wrapper around the AWS REST API.
This isn't particular to Go; it is true for at least the Java SDK as well. However, the verbosity required to use it
for even simple tasks feels out of place in Go.

Also, it is sometimes good to view a DynamoDB table as simply a cloud-based hashmap. This view may lead you away from
awkward designs that can arise from trying to force a traditional database mindset into the DynamoDB storage model,
such as a proliferation of global secondary indexes or overuse of scanning.

This library intentionally ignores some of the features of DynamoDB, such as range key queries and batching,
to provide a simplified API.

* Get a single record
* Put a single record
* Delete a single record
* Conditional Put (if absent, or by numerical version)
* Iterate over all records (optionally in parallel)


# Map API
The reflection-based API `ddbmap.Map` requires very little code to use, but be aware that
you must either accept capitalized DynamoDB field names, or use dynamo struct tags to rename exported fields.
This API has the advantage that users can use `*sync.Map` instead of DynamoDB Local for unit testing.
It has the disadvantage that it cannot tolerate AWS SDK errors, and will panic if they occur. Users are advised to
handle panics with `recover`, and at least ensure the SDK will always retry the usual errors like throttling.

Map API Usage
-------------
Users getting started with ddbmap might also reference the `ddbmap/examples` package.

```go
package main

import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/aws/external"
    "github.com/shawnsmithdev/ddbmap"
    "fmt"
)

type PersonKey struct {
    Id int
}

type Person struct {
    PersonKey
    Name string
    Age  int
}


func main() {
    awsCfg, _ := external.LoadDefaultAWSConfig()
    awsCfg.Retryer = aws.DefaultRetryer{NumMaxRetries: 1000}

    // Assumes table already exists, will auto-discover key names
    tCfg := ddbmap.TableConfig{
        TableName:         "TestTable",
        ValueUnmarshaller: ddbmap.UnmarshallerForType(Person{}),
    }
    people, _ := tCfg.NewMap(awsCfg)

    // put
    p1 := Person{PersonKey: PersonKey{Id: 1}, Name: "Bob", Age: 20}
    people.Store(p1.PersonKey, p1)

    // get
    p2, ok := people.Load(p1.PersonKey)
    if ok {
        fmt.Println(p2.(Person))
    }

    // iterate
    people.Range(func(_, p3 interface{}) bool {
        fmt.Println(p3.(Person))
        return true
    })
}
```

# Item API
As an alternative approach, the `ddbmap.ItemMap` API may be used with some more effort by implementing `ddbmap.Itemable`
to handle conversions between the Go and DynamoDB type systems directly, without using reflection.
All methods that take `Itemable` will return an `error` and will not panic.

Doing these kinds of type conversions can be tedious and hard to read, so a utility library is provided
in `ddbmap/ddbconv` to help users implement `Itemable`.

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
