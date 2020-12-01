package schema

import (
	"elastic-transfer/config/options"
	"os"
	"testing"
)

var schema *Schema

func TestMain(m *testing.M) {
	os.Chdir("../../..")
	schema = New("./config/autoload/")
	os.Exit(m.Run())
}

func TestSchema_Update(t *testing.T) {
	err := schema.Update(options.PipeOption{
		Identity: "debug",
		Index:    "debug-log",
		Validate: `{"type":"object","properties":{"name":{"type":"string"}}}`,
		Topic:    "sys.debug",
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
	err := schema.Delete("debug")
	if err != nil {
		t.Fatal(err)
	}
}
