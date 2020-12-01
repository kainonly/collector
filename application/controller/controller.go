package controller

import (
	pb "elastic-collector/api"
	"elastic-collector/application/common"
	"elastic-collector/config/options"
)

type controller struct {
	pb.UnimplementedAPIServer
	*common.Dependency
}

func New(dep *common.Dependency) *controller {
	c := new(controller)
	c.Dependency = dep
	return c
}

func (c *controller) find(identity string) (_ *pb.Data, err error) {
	var pipe *options.PipeOption
	if pipe, err = c.Collector.GetPipe(identity); err != nil {
		return
	}
	return &pb.Data{
		Id:    pipe.Identity,
		Index: pipe.Index,
		Queue: pipe.Queue,
	}, nil
}
