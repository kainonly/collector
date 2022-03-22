package bootstrap

import (
	"errors"
	"fmt"
	"github.com/google/wire"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/weplanx/collector/common"
	"github.com/weplanx/collector/utiliy"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

var Provides = wire.NewSet(
	UseZap,
	UseNats,
	UseJetStream,
	UseLogSystem,
)

// SetValues 初始化配置
func SetValues() (values *common.Values, err error) {
	if _, err = os.Stat("./config/config.yml"); os.IsNotExist(err) {
		err = errors.New("静态配置不存在，请检查路径 [./config/config.yml]")
		return
	}
	var b []byte
	b, err = ioutil.ReadFile("./config/config.yml")
	if err != nil {
		return
	}
	err = yaml.Unmarshal(b, &values)
	if err != nil {
		return
	}
	return
}

func UseZap() (log *zap.Logger, err error) {
	if log, err = zap.NewProduction(); err != nil {
		return
	}
	return
}

func UseNats(values *common.Values) (nc *nats.Conn, err error) {
	var kp nkeys.KeyPair
	if kp, err = nkeys.FromSeed([]byte(values.Nats.Nkey)); err != nil {
		return
	}
	defer kp.Wipe()
	var pub string
	if pub, err = kp.PublicKey(); err != nil {
		return
	}
	if !nkeys.IsValidPublicUserKey(pub) {
		return nil, fmt.Errorf("nkey 验证失败")
	}
	if nc, err = nats.Connect(
		strings.Join(values.Nats.Hosts, ","),
		nats.MaxReconnects(5),
		nats.ReconnectWait(2*time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 2*time.Second),
		nats.Nkey(pub, func(nonce []byte) ([]byte, error) {
			sig, _ := kp.Sign(nonce)
			return sig, nil
		}),
	); err != nil {
		return
	}
	return
}

func UseJetStream(nc *nats.Conn) (nats.JetStreamContext, error) {
	return nc.JetStream(nats.PublishAsyncMaxPending(256))
}

func UseLogSystem(values *common.Values, log *zap.Logger) (*utiliy.LogSystem, error) {
	return utiliy.NewLogSystem(values.LogSystem, log)
}
