package controller

import (
	"context"
	"elastic-collector/app/types"
	pb "elastic-collector/router"
)

func (c *controller) Put(ctx context.Context, param *pb.Information) (*pb.Response, error) {
	err := c.manager.Put(types.PipeOption{
		Identity: param.Identity,
		Index:    param.Index,
		Queue:    param.Queue,
	})
	if err != nil {
		return c.response(err)
	}
	return c.response(nil)
}
