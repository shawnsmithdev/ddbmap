[![GoDoc](https://godoc.org/github.com/shawnsmithdev/ddbmap?status.png)](https://godoc.org/github.com/shawnsmithdev/ddbmap)
[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/shawnsmithdev/ddbmap/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/shawnsmithdev/ddbmap)](https://goreportcard.com/report/github.com/shawnsmithdev/ddbmap)

# ddbmap
ddbmap is a Go (golang) library that presents a map-like view of an AWS DynamoDB table (using AWS Go SDK v2).

It is not complete. Until a commit is tagged, the API may be broken or changed for any reason without notice.

# Motivation
The AWS Go SDK is fairly low level. It acts as a kind of wrapper around the AWS REST API.
This isn't particular to Go; it is true for at least the Java SDK as well. However, the verbosity required to use it
for even simple tasks feels out of place in Go.

This library ignores some of the features of DynamoDB, such as range key queries and global secondary indexes,
to provide a simplified API for users that only need a limited subset of DynamoDB's features.

* Get a single record
* Put a single record
* Delete a single record
* Conditional Put If Absent
* Iterate over all records (serially or in parallel)

# Choice of API
Users may choose to use the reflection-based API `ddbmap.Map` with very little code required, but be aware that
you must either accept capitalized DynamoDB field names, or use dynamo struct tags to rename exported fields.
This API has the advantage that users can use `*sync.Map` instead of DynamoDB Local for unit testing.
It has the disadvantage that it cannot tolerate AWS SDK errors, and will panic if they occur. Users are advised to
handle panics with `recover`, and at least ensure the SDK will always retry the usual errors like throttling.

As an alternative approach, the `ddbmap.ItemMap` API may be used with some more effort by implementing `ddbmap.Itemable`
to handle conversions between the Go and DynamoDB type systems directly, without using reflection.
All methods that take `Itemable` will return an `error` and will not panic. This API also provides a few additional
conditional operations with no analogue in `ddbmap.Map` / `*sync.Map`.

Doing these kinds of type conversions can be tedious and hard to read, so a utility library is provided
in `ddbmap/ddbconv` to help users implement `Itemable`.

# Usage
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
        AWSConfig: awsCfg,
        TableName: "TestTable",
    }
    people, _ := tCfg.NewItemMap()

    // put
    bob := Person{PersonKey: {Id: 1}, Name: "Bob", Age: 20}
    people.Store(bob.PersonKey, bob)

    // get
    people.Load(bob.PersonKey)

    // iterate
    people.Range(func(_, person interface{}) bool {
        fmt.Println(person.(*Person))
        return true
    })
}
```
