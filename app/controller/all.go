package controller

import (
	"context"
	pb "elastic-collector/router"
)

func (c *controller) All(ctx context.Context, _ *pb.NoParameter) (*pb.AllResponse, error) {
	return &pb.AllResponse{
		Error: 0,
		Data:  c.manager.GetIdentityCollection(),
	}, nil
}
