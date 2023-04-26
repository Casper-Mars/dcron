package redis

import (
	"context"
	"fmt"
	"github.com/Casper-Mars/dcron/node"
	"log"
	"time"

	"github.com/Casper-Mars/dcron/dlog"
	"github.com/Casper-Mars/dcron/driver"
	"github.com/go-redis/redis/v8"
)

// Driver is Driver
type Driver struct {
	client  *redis.Client
	timeout time.Duration
	Key     string
	logger  dlog.Logger
}

// NewDriver return a redis driver
func NewDriver(opts *redis.Options) (*Driver, error) {
	return &Driver{
		client: redis.NewClient(opts),
		logger: &dlog.StdLogger{
			Log: log.Default(),
		},
	}, nil
}

// Ping is check redis valid
func (rd *Driver) Ping() error {
	reply, err := rd.client.Ping(context.Background()).Result()
	if err != nil {
		return err
	}
	if reply != "PONG" {
		return fmt.Errorf("Ping received is error, %s", string(reply))
	}
	return err
}

// SetTimeout set redis timeout
func (rd *Driver) SetTimeout(timeout time.Duration) {
	rd.timeout = timeout
}

// SetHeartBeat set heatbeat
func (rd *Driver) SetHeartBeat(node *node.Node) {
	go rd.heartBeat(node)
}
func (rd *Driver) heartBeat(node *node.Node) {

	//每间隔timeout/2设置一次key的超时时间为timeout
	key := node.ID
	tickers := time.NewTicker(rd.timeout / 2)
	for range tickers.C {
		keyExist, err := rd.client.Expire(context.Background(), key, rd.timeout).Result()
		if err != nil {
			rd.logger.Errorf("redis expire error %+v", err)
			continue
		}
		if !keyExist {
			if err := rd.registerServiceNode(node); err != nil {
				rd.logger.Errorf("register service node error %+v", err)
			}
		}
	}
}

func (rd *Driver) SetLogger(log dlog.Logger) {
	rd.logger = log
}

// GetServiceNodeList get a serveice node  list
func (rd *Driver) GetServiceNodeList(serviceName string) ([]*node.Node, error) {
	mathStr := fmt.Sprintf("%s*", driver.GetKeyPre(serviceName))
	services, err := rd.scan(mathStr)
	if err != nil {
		return nil, err
	}
	result := make([]*node.Node, len(services))
	for i, service := range services {
		result[i] = &node.Node{}
		if err := result[i].UnmarshalBinary([]byte(service)); err != nil {
			return nil, fmt.Errorf("[redis]node unmarshal error: %w, node: %s", err, service)
		}
	}
	return result, err
}

// RegisterServiceNode  register a service node
func (rd *Driver) RegisterServiceNode(node *node.Node) (nodeID string, err error) {
	node.ID = driver.GetNodeId(node.ServiceName)
	if err := rd.registerServiceNode(node); err != nil {
		return "", err
	}
	return node.ID, nil
}

func (rd *Driver) registerServiceNode(node *node.Node) error {
	bytes, err := node.MarshalBinary()
	if err != nil {
		return fmt.Errorf("[redis]node marshal error: %w", err)
	}
	return rd.client.SetEX(context.Background(), node.ID, string(bytes), rd.timeout).Err()
}

func (rd *Driver) scan(matchStr string) ([]string, error) {
	keys := make([]string, 0)
	ctx := context.Background()
	iter := rd.client.Scan(ctx, 0, matchStr, -1).Iterator()
	for iter.Next(ctx) {
		err := iter.Err()
		if err != nil {
			return nil, fmt.Errorf("[redis]scan key error: %w", err)
		}
		keys = append(keys, iter.Val())
	}
	if len(keys) == 0 {
		return []string{}, nil
	}
	result, err := rd.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("[redis]mget error: %w", err)
	}
	vals := make([]string, 0, len(result))
	for _, v := range result {
		if v != nil {
			vals = append(vals, v.(string))
		}
	}
	return vals, nil
}
