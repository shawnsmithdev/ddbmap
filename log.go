package ddbmap

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
)

func logErr(err error, logger aws.LoggerFunc) {
	e := err
	for {
		logger(e.Error())
		if aerr, ok := e.(awserr.Error); ok {
			if aerr.OrigErr() == nil {
				return
			}
			logger("caused by:")
			e = aerr.OrigErr()
		} else {
			return
		}
	}
}

func getErrCode(err error) string {
	if aerr, ok := err.(awserr.Error); ok {
		return aerr.Code()
	}
	return ""
}

// Only use if documented to panic or when err can only be due to a library bug
func forbidErr(err error, logger aws.LoggerFunc) {
	if err != nil {
		logErr(err, logger)
		logger("unhandled error, will now panic")
		panic(err)
	}
}
