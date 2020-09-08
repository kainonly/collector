package schema

import (
	"elastic-transfer/app/types"
	"encoding/json"
	"os"
	"testing"
)

var schema *Schema

func TestMain(m *testing.M) {
	os.Chdir("../..")
	schema = New()
	os.Exit(m.Run())
}

func TestSchema_Update(t *testing.T) {
	var err error
	var body1 interface{}
	err = json.Unmarshal([]byte(`{"name":"task1"}`), &body1)
	if err != nil {
		t.Fatal(err)
	}
	var body2 interface{}
	err = json.Unmarshal([]byte(`{"name":"task2"}`), &body2)
	if err != nil {
		t.Fatal(err)
	}
	err = schema.Update(types.PipeOption{
		Identity: "task",
		Index:    "task-log",
		Validate: `{"type":"object","properties":{"name":{"type":"string"}}}`,
		Topic:    "sys.schedule",
		Key:      "",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSchema_Lists(t *testing.T) {
	_, err := schema.Lists()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSchema_Delete(t *testing.T) {
	err := schema.Delete("task")
	if err != nil {
		t.Fatal(err)
	}
}
