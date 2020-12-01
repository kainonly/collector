package controller

import (
	"context"
	pb "elastic-collector/api"
	"elastic-collector/config/options"
	"github.com/golang/protobuf/ptypes/empty"
)

func (c *controller) Put(_ context.Context, Data *pb.Data) (_ *empty.Empty, err error) {
	if err = c.Collector.Put(options.PipeOption{
		Identity: Data.Id,
		Index:    Data.Index,
		Queue:    Data.Queue,
	}); err != nil {
		return
	}
	return &empty.Empty{}, nil
}
