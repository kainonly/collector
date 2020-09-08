package controller

import (
	"context"
	"elastic-collector/app/types"
	pb "elastic-collector/router"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

var client pb.RouterClient

func TestMain(m *testing.M) {
	os.Chdir("../..")
	var err error
	if _, err := os.Stat("./config/autoload"); os.IsNotExist(err) {
		os.Mkdir("./config/autoload", os.ModeDir)
	}
	cfgByte, err := ioutil.ReadFile("./config/config.yml")
	if err != nil {
		log.Fatalln("Failed to read service configuration file", err)
	}
	config := types.Config{}
	err = yaml.Unmarshal(cfgByte, &config)
	if err != nil {
		log.Fatalln("Service configuration file parsing failed", err)
	}
	grpcConn, err := grpc.Dial(config.Listen, grpc.WithInsecure())
	if err != nil {
		logrus.Fatalln(err)
	}
	client = pb.NewRouterClient(grpcConn)
	os.Exit(m.Run())
}

func TestController_Put(t *testing.T) {
	response, err := client.Put(context.Background(), &pb.Information{
		Identity: "task-1",
		Index:    "task-1",
		Queue:    `schedule`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != 0 {
		t.Error(response.Msg)
	} else {
		t.Log(response.Msg)
	}
}

func TestController_All(t *testing.T) {
	response, err := client.All(context.Background(), &pb.NoParameter{})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != 0 {
		t.Error(response.Msg)
	} else {
		t.Log(response.Msg)
	}
}

func TestController_Get(t *testing.T) {
	response, err := client.Get(context.Background(), &pb.GetParameter{
		Identity: "task-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != 0 {
		t.Error(response.Msg)
	} else {
		t.Log(response.Msg)
	}
}

func TestController_Lists(t *testing.T) {
	response, err := client.Lists(context.Background(), &pb.ListsParameter{
		Identity: []string{"task-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != 0 {
		t.Error(response.Msg)
	} else {
		t.Log(response.Msg)
	}
}

func TestController_Delete(t *testing.T) {
	response, err := client.Delete(context.Background(), &pb.DeleteParameter{
		Identity: "task-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != 0 {
		t.Error(response.Msg)
	} else {
		t.Log(response.Msg)
	}
	response, err = client.Delete(context.Background(), &pb.DeleteParameter{
		Identity: "task-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != 0 {
		t.Error(response.Msg)
	} else {
		t.Log(response.Msg)
	}
}
