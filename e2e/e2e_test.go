package e2e_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/kainonly/collector/v3/transfer"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

var nc *nats.Conn
var x *transfer.Transfer

func TestMain(m *testing.M) {
	// 加载 .env 文件（如果存在）
	_ = godotenv.Load("../.env")

	// 从环境变量获取配置
	natsHosts := os.Getenv("NATS_HOSTS")
	if natsHosts == "" {
		fmt.Fprintln(os.Stderr, "E2E 测试已跳过（缺少 NATS_HOSTS 环境变量）")
		os.Exit(0)
	}
	natsToken := os.Getenv("NATS_TOKEN")
	namespace := os.Getenv("NATS_NAMESPACE")
	if namespace == "" {
		namespace = "test"
	}

	var err error
	if nc, err = nats.Connect(
		strings.Join(strings.Split(natsHosts, ","), ","),
		nats.Token(natsToken),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(3),
	); err != nil {
		panic(err)
	}
	defer nc.Close()

	// 创建 JetStream 和 KV 存储桶
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 确保 KV 存储桶存在
	if _, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      namespace,
		Description: "E2E test bucket",
		History:     3,
		Compression: true,
	}); err != nil {
		panic(err)
	}

	x, err = transfer.New(ctx, namespace, nc)
	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func TestTransfer_Add(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		key := "example"
		if i > 0 {
			key = fmt.Sprintf(`%s_%d`, key, i)
		}
		err := x.Add(ctx, transfer.Option{
			Key:         key,
			Description: "Example stream",
			Collection:  fmt.Sprintf(`%ss_%d`, key, i),
		})
		assert.NoError(t, err)
		t.Logf("Added key: %s", key)
	}
}

func TestTransfer_Get(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		key := "example"
		if i > 0 {
			key = fmt.Sprintf(`%s_%d`, key, i)
		}
		option, err := x.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, key, option.Key)
		t.Logf("Got key: %s, Option: %+v", key, option)
	}
}

func TestTransfer_Send(t *testing.T) {
	key := "example"
	data := map[string]any{
		"name":       "test",
		"created_at": time.Now(),
	}
	err := x.Send(key, data)
	assert.NoError(t, err)
	<-x.Js.PublishAsyncComplete()
}

func TestTransfer_Remove(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		key := "example"
		if i > 0 {
			key = fmt.Sprintf(`%s_%d`, key, i)
		}
		err := x.Remove(ctx, key)
		assert.NoError(t, err)
		t.Logf("Removed key: %s", key)
	}
}

func TestTransfer_SendBulk(t *testing.T) {
	key := "example"
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	count := 0

	for {
		select {
		case <-ticker.C:
			// 每秒发送1500条数据
			for i := 0; i < 1500; i++ {
				data := map[string]any{
					"id":         count,
					"name":       "bulk_test",
					"created_at": time.Now(),
				}
				err := x.Send(key, data)
				assert.NoError(t, err)
				count++
			}
			t.Logf("Sent %d records so far", count)
		case <-timeout.C:
			t.Logf("Test completed. Total records sent: %d", count)
			<-x.Js.PublishAsyncComplete()
			return
		}
	}
}

func TestTransfer_SendBulkMultiKeys(t *testing.T) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	counts := make(map[string]int)
	for i := 0; i < 10; i++ {
		key := "example"
		if i > 0 {
			key = fmt.Sprintf(`%s_%d`, key, i)
		}
		counts[key] = 0
	}

	for {
		select {
		case <-ticker.C:
			// 10个key同时push数据，每个key的数量不同
			for i := 0; i < 10; i++ {
				key := "example"
				if i > 0 {
					key = fmt.Sprintf(`%s_%d`, key, i)
				}
				// 每个key的数据量不同，便于检查
				recordsPerKey := 1000 + i*50
				for j := 0; j < recordsPerKey; j++ {
					data := map[string]any{
						"id":         counts[key],
						"key":        key,
						"name":       "multi_bulk_test",
						"created_at": time.Now(),
					}
					err := x.Send(key, data)
					assert.NoError(t, err)
					counts[key]++
				}
			}
			totalCount := 0
			for key, count := range counts {
				totalCount += count
				t.Logf("Key %s: %d records", key, count)
			}
			t.Logf("Total records sent: %d", totalCount)
		case <-timeout.C:
			t.Log("\n=== Test Summary ===")
			totalCount := 0
			for i := 0; i < 10; i++ {
				key := "example"
				if i > 0 {
					key = fmt.Sprintf(`%s_%d`, key, i)
				}
				count := counts[key]
				totalCount += count
				t.Logf("Key %s: %d records", key, count)
			}
			t.Logf("Test completed. Total records sent: %d", totalCount)
			<-x.Js.PublishAsyncComplete()
			return
		}
	}
}
