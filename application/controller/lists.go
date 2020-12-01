package controller

import (
	"context"
	pb "elastic-collector/api"
)

func (c *controller) Lists(_ context.Context, option *pb.IDs) (_ *pb.DataLists, err error) {
	lists := make([]*pb.Data, len(option.Ids))
	for key, val := range option.Ids {
		if lists[key], err = c.find(val); err != nil {
			return
		}
	}
	return &pb.DataLists{Data: lists}, nil
}
