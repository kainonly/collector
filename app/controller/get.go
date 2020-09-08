package controller

import (
	"context"
	pb "elastic-collector/router"
)

func (c *controller) Get(ctx context.Context, param *pb.GetParameter) (*pb.GetResponse, error) {
	information, err := c.find(param.Identity)
	if err != nil {
		return c.getErrorResponse(err)
	}
	return c.getSuccessResponse(information)
}

func (c *controller) getErrorResponse(err error) (*pb.GetResponse, error) {
	return &pb.GetResponse{
		Error: 1,
		Msg:   err.Error(),
	}, nil
}

func (c *controller) getSuccessResponse(data *pb.Information) (*pb.GetResponse, error) {
	return &pb.GetResponse{
		Error: 0,
		Msg:   "ok",
		Data:  data,
	}, nil
}
