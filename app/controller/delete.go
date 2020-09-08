package controller

import (
	pb "elastic-collector/router"
	"golang.org/x/net/context"
)

func (c *controller) Delete(ctx context.Context, param *pb.DeleteParameter) (*pb.Response, error) {
	err := c.manager.Delete(param.Identity)
	if err != nil {
		return c.response(err)
	}
	return c.response(nil)
}
