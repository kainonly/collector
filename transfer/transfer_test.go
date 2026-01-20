package transfer_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/joho/godotenv"
	"github.com/kainonly/collector/v3/transfer"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	x  *transfer.Transfer
	nc *nats.Conn
	js jetstream.JetStream
	mc *mongo.Client
	db *mongo.Database
)

var (
	namespace = getenvDefault("NATS_NAMESPACE", "test")
	seq       uint64
)

func TestMain(m *testing.M) {
	// 加载 .env 文件（如果存在）
	_ = godotenv.Load("../.env")

	if os.Getenv("NATS_HOSTS") == "" {
		fmt.Fprintln(os.Stderr, "集成测试已跳过（缺少 NATS_HOSTS 环境变量）")
		os.Exit(0)
	}
	if os.Getenv("MONGO_URL") == "" {
		fmt.Fprintln(os.Stderr, "集成测试已跳过（缺少 MONGO_URL 环境变量）")
		os.Exit(0)
	}

	var err error
	if err = setup(); err != nil {
		panic(err)
	}

	code := m.Run()

	teardown()
	os.Exit(code)
}

func getenvDefault(key, value string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return value
}

func nextKey(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, atomic.AddUint64(&seq, 1))
}

func setup() (err error) {
	ctx := context.Background()

	// 连接 NATS
	if nc, err = nats.Connect(
		os.Getenv("NATS_HOSTS"),
		nats.Token(os.Getenv("NATS_TOKEN")),
		nats.Timeout(5*time.Second),
	); err != nil {
		return fmt.Errorf("连接 NATS 失败: %w", err)
	}

	// 创建 JetStream
	if js, err = jetstream.New(nc); err != nil {
		return fmt.Errorf("创建 JetStream 失败: %w", err)
	}

	// 创建 KV 存储桶
	if _, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      namespace,
		Description: "测试存储桶",
		History:     3,
		Compression: true,
	}); err != nil {
		return fmt.Errorf("创建 KV 存储桶失败: %w", err)
	}

	// 连接 MongoDB
	if mc, err = mongo.Connect(options.Client().ApplyURI(os.Getenv("MONGO_URL"))); err != nil {
		return fmt.Errorf("连接 MongoDB 失败: %w", err)
	}

	dbName := os.Getenv("MONGO_DATABASE")
	if dbName == "" {
		dbName = "test"
	}
	db = mc.Database(dbName)

	// 创建 Transfer
	if x, err = transfer.New(ctx, namespace, nc); err != nil {
		return fmt.Errorf("创建 Transfer 失败: %w", err)
	}

	return nil
}

func teardown() {
	ctx := context.Background()

	// 清理 KV 存储桶
	_ = js.DeleteKeyValue(ctx, namespace)

	// 关闭连接
	if mc != nil {
		_ = mc.Disconnect(ctx)
	}
	if nc != nil {
		nc.Close()
	}
}

// MsgData 测试消息结构
type MsgData struct {
	ID        bson.ObjectID `bson:"_id"`
	Timestamp time.Time     `bson:"timestamp"`
	Msg       string        `bson:"msg"`
	Value     float64       `bson:"value"`
}

func TestTransfer_Add(t *testing.T) {
	ctx := context.Background()

	key := nextKey("metrics")
	err := x.Add(ctx, transfer.Option{
		Key:         key,
		Collection:  key,
		Description: "指标流",
	})
	assert.NoError(t, err)

	// 验证流已创建
	stream, err := js.Stream(ctx, fmt.Sprintf("%s_%s", namespace, key))
	assert.NoError(t, err)
	assert.NotNil(t, stream)

	info, err := stream.Info(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "指标流", info.Config.Description)

	_ = x.Remove(ctx, key)
	_ = js.DeleteStream(ctx, fmt.Sprintf("%s_%s", namespace, key))
}

func TestTransfer_Send(t *testing.T) {
	ctx := context.Background()

	key := nextKey("metrics")
	err := x.Add(ctx, transfer.Option{
		Key:         key,
		Collection:  key,
		Description: "指标流",
	})
	assert.NoError(t, err)

	// 发送多条消息
	for i := 0; i < 10; i++ {
		err := x.Send(key, MsgData{
			ID:        bson.NewObjectID(),
			Timestamp: time.Now(),
			Msg:       fmt.Sprintf("测试消息 %d", i),
			Value:     float64(i) * 0.1,
		})
		assert.NoError(t, err)
	}

	// 等待异步发布完成
	select {
	case <-x.Js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatal("发布超时")
	}

	// 验证消息已写入流
	stream, err := js.Stream(ctx, fmt.Sprintf("%s_%s", namespace, key))
	assert.NoError(t, err)

	info, err := stream.Info(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), info.State.Msgs)

	_ = x.Remove(ctx, key)
	_ = js.DeleteStream(ctx, fmt.Sprintf("%s_%s", namespace, key))
}

func TestTransfer_Consume(t *testing.T) {
	ctx := context.Background()

	key := nextKey("metrics")
	err := x.Add(ctx, transfer.Option{
		Key:         key,
		Collection:  key,
		Description: "指标流",
	})
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = x.Send(key, MsgData{
			ID:        bson.NewObjectID(),
			Timestamp: time.Now(),
			Msg:       fmt.Sprintf("测试消息 %d", i),
			Value:     float64(i) * 0.1,
		})
		assert.NoError(t, err)
	}
	select {
	case <-x.Js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatal("发布超时")
	}

	// 获取消费者
	consumer, err := js.Consumer(ctx, fmt.Sprintf("%s_%s", namespace, key), "default")
	assert.NoError(t, err)

	// 消费消息并写入 MongoDB
	batch, err := consumer.FetchNoWait(10)
	assert.NoError(t, err)

	var documents []any
	var msgs []jetstream.Msg
	for msg := range batch.Messages() {
		documents = append(documents, msg.Data())
		msgs = append(msgs, msg)
	}

	assert.Len(t, documents, 10)

	// 写入 MongoDB
	_, err = db.Collection(key).InsertMany(ctx, documents)
	assert.NoError(t, err)

	// ACK 消息
	for _, msg := range msgs {
		err = msg.Ack()
		assert.NoError(t, err)
	}

	// 验证 MongoDB 中的数据
	count, err := db.Collection(key).CountDocuments(ctx, bson.M{})
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)

	_ = db.Collection(key).Drop(ctx)
	_ = x.Remove(ctx, key)
	_ = js.DeleteStream(ctx, fmt.Sprintf("%s_%s", namespace, key))
}

func TestTransfer_Remove(t *testing.T) {
	ctx := context.Background()

	key := nextKey("metrics")
	err := x.Add(ctx, transfer.Option{
		Key:         key,
		Collection:  key,
		Description: "指标流",
	})
	assert.NoError(t, err)

	err = x.Remove(ctx, key)
	assert.NoError(t, err)

	// 验证 KV 已删除
	_, err = x.Kv.Get(ctx, key)
	assert.Error(t, err)

	// 清理 MongoDB 集合
	_ = db.Collection(key).Drop(ctx)

	// 清理流
	_ = js.DeleteStream(ctx, fmt.Sprintf("%s_%s", namespace, key))
}

func TestTransfer_AddWithSubs(t *testing.T) {
	ctx := context.Background()

	// 创建带有额外订阅主题的流
	key := nextKey("events")
	err := x.Add(ctx, transfer.Option{
		Key:         key,
		Subs:        []string{"external.events.>"},
		Collection:  key,
		Description: "事件流",
	})
	assert.NoError(t, err)

	// 验证流配置
	stream, err := js.Stream(ctx, fmt.Sprintf("%s_%s", namespace, key))
	assert.NoError(t, err)

	info, err := stream.Info(ctx)
	assert.NoError(t, err)
	assert.Contains(t, info.Config.Subjects, fmt.Sprintf("%s.%s", namespace, key))
	assert.Contains(t, info.Config.Subjects, "external.events.>")

	// 清理
	_ = x.Remove(ctx, key)
	_ = js.DeleteStream(ctx, fmt.Sprintf("%s_%s", namespace, key))
}

func TestTransfer_Get(t *testing.T) {
	ctx := context.Background()

	key := nextKey("state")
	err := x.Add(ctx, transfer.Option{
		Key:         key,
		Collection:  key,
		Description: "状态流",
	})
	assert.NoError(t, err)

	sub, err := nc.Subscribe(fmt.Sprintf("%s.states", namespace), func(m *nats.Msg) {
		b, errX := sonic.Marshal(&transfer.State{BufferSize: 7})
		assert.NoError(t, errX)
		_ = nc.Publish(m.Reply, b)
	})
	assert.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	option, err := x.Get(ctx, key)
	assert.NoError(t, err)
	assert.NotNil(t, option)
	assert.NotNil(t, option.State)
	assert.Equal(t, 7, option.State.BufferSize)

	_ = x.Remove(ctx, key)
	_ = js.DeleteStream(ctx, fmt.Sprintf("%s_%s", namespace, key))
}

func TestTransfer_InvalidNamespace(t *testing.T) {
	ctx := context.Background()

	// 测试包含连字符的命名空间
	_, err := transfer.New(ctx, "invalid-namespace", nc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "-")
}
