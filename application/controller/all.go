package controller

import (
	"context"
	pb "elastic-collector/api"
	"github.com/golang/protobuf/ptypes/empty"
)

func (c *controller) All(_ context.Context, _ *empty.Empty) (*pb.IDs, error) {
	var ids []string
	for id, _ := range c.Collector.Pipes.Lists() {
		ids = append(ids, id)
	}
	return &pb.IDs{Ids: ids}, nil
}
