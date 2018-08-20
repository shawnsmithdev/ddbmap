package ddbmap

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type scanWorker struct {
	workerId int64
	input    *ddb.ScanInput
	table    *DynamoMap
	consumer func(Item) bool
	ctx      context.Context
}

func (s scanWorker) withId(workerId int, input ddb.ScanInput) *scanWorker {
	s.workerId = int64(workerId)
	input.Segment = &s.workerId
	s.input = &input
	return &s
}

func (s *scanWorker) debug(input ...interface{}) {
	s.table.debug(append(input, "worker:", s.workerId)...)
}

func (s *scanWorker) work() error {
	s.debug("starting scan")
	for {
		// fetch a page
		s.debug("scan request input:", s.input)
		resp, err := s.table.Client.ScanRequest(s.input).Send()
		s.debug("scan response:", resp, "error:", err)
		if err != nil {
			return err
		}
		// run consumer on each record in page
		for _, item := range resp.Items {
			if !s.consumer(item) {
				s.debug("scan worker received early termination")
				return errEarlyTermination
			}
		}
		if resp.LastEvaluatedKey == nil {
			s.debug("scan done")
			return nil
		}
		if s.ctx != nil {
			if err := s.ctx.Err(); err != nil {
				s.debug("scan worker peer early termination, err:", err)
				return errEarlyTermination
			}
		}
		s.input.ExclusiveStartKey = resp.LastEvaluatedKey
	}
}
