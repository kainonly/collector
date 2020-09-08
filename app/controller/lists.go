package controller

import (
	"context"
	pb "elastic-collector/router"
)

func (c *controller) Lists(ctx context.Context, param *pb.ListsParameter) (*pb.ListsResponse, error) {
	lists := make([]*pb.Information, len(param.Identity))
	for index, identity := range param.Identity {
		infomation, err := c.find(identity)
		if err != nil {
			return c.listsErrorResponse(err)
		}
		lists[index] = infomation
	}
	return c.listsSuccessResponse(lists)
}

func (c *controller) listsErrorResponse(err error) (*pb.ListsResponse, error) {
	return &pb.ListsResponse{
		Error: 1,
		Msg:   err.Error(),
	}, nil
}

func (c *controller) listsSuccessResponse(data []*pb.Information) (*pb.ListsResponse, error) {
	return &pb.ListsResponse{
		Error: 0,
		Msg:   "ok",
		Data:  data,
	}, nil
}
