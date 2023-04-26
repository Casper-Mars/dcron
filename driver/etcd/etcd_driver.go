package etcd

import (
	"context"
	"fmt"
	"github.com/Casper-Mars/dcron/node"
	"log"
	"sync"
	"time"

	"github.com/Casper-Mars/dcron/dlog"
	"github.com/Casper-Mars/dcron/driver"
	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var _ driver.Driver = &EtcdDriver{}

const (
	defaultLease    = 5 // 5 second ttl
	dialTimeout     = 3 * time.Second
	businessTimeout = 5 * time.Second
)

type EtcdDriver struct {
	cli        *clientv3.Client
	lease      int64
	serverList map[string]map[string]string
	lock       sync.RWMutex
	leaseID    clientv3.LeaseID
	logger     dlog.Logger
}

// NewEtcdDriver ...
func NewEtcdDriver(config *clientv3.Config) (*EtcdDriver, error) {
	cli, err := clientv3.New(*config)
	if err != nil {
		return nil, err
	}

	ser := &EtcdDriver{
		cli:        cli,
		serverList: make(map[string]map[string]string, 10),
		logger: &dlog.StdLogger{
			Log: log.Default(),
		},
	}

	return ser, nil
}

// 设置key value，绑定租约
func (s *EtcdDriver) putKeyWithLease(key, val string) (clientv3.LeaseID, error) {
	//设置租约时间，最少5s
	if s.lease < defaultLease {
		s.lease = defaultLease
	}

	ctx, cancel := context.WithTimeout(context.Background(), businessTimeout)
	defer cancel()

	resp, err := s.cli.Grant(ctx, s.lease)
	if err != nil {
		return 0, err
	}
	//注册服务并绑定租约
	_, err = s.cli.Put(ctx, key, val, clientv3.WithLease(resp.ID))
	if err != nil {
		return 0, err
	}

	return resp.ID, nil
}

func (s *EtcdDriver) randNodeID(serviceName string) (nodeID string) {
	return getPrefix(serviceName) + uuid.New().String()
}

// WatchService 初始化服务列表和监视
func (s *EtcdDriver) watchService(serviceName string) error {
	prefix := getPrefix(serviceName)
	// 根据前缀获取现有的key
	resp, err := s.cli.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, ev := range resp.Kvs {
		s.setServiceList(serviceName, string(ev.Key), string(ev.Value))
	}

	// 监视前缀，修改变更的server
	go s.watcher(serviceName)
	return nil
}

func getPrefix(serviceName string) string {
	return serviceName + "/"
}

// watcher 监听前缀
func (s *EtcdDriver) watcher(serviceName string) {
	prefix := getPrefix(serviceName)
	rch := s.cli.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT: //修改或者新增
				s.setServiceList(serviceName, string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE: //删除
				s.delServiceList(serviceName, string(ev.Kv.Key))
			}
		}
	}
}

// setServiceList 新增服务地址
func (s *EtcdDriver) setServiceList(serviceName, key, val string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.serverList[serviceName]; !ok {
		nodeMap := map[string]string{
			key: key,
		}
		s.serverList[serviceName] = nodeMap
	} else {
		s.serverList[serviceName][key] = val
	}
}

// DelServiceList 删除服务地址
func (s *EtcdDriver) delServiceList(serviceName, key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if nodeMap, ok := s.serverList[serviceName]; ok {
		delete(nodeMap, key)
	}
}

// GetServices 获取服务地址
func (s *EtcdDriver) getServices(serviceName string) []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	addrs := make([]string, 0)
	if nodeMap, ok := s.serverList[serviceName]; ok {
		for _, v := range nodeMap {
			addrs = append(addrs, v)
		}
	}
	return addrs
}

func (e *EtcdDriver) Ping() error {
	return nil
}

func (e *EtcdDriver) keepAlive(ctx context.Context, node *node.Node) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	bytes, err := node.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal node error: %w", err)
	}

	e.leaseID, err = e.putKeyWithLease(node.ID, string(bytes))
	if err != nil {
		e.logger.Errorf("putKeyWithLease error: %v", err)
		return nil, err
	}

	return e.cli.KeepAlive(ctx, e.leaseID)
}

func (e *EtcdDriver) revoke() {
	_, err := e.cli.Lease.Revoke(context.Background(), e.leaseID)
	if err != nil {
		log.Printf("lease revoke error: %v", err)
	}
}

func (e *EtcdDriver) SetHeartBeat(node *node.Node) {
	leaseCh, err := e.keepAlive(context.Background(), node)
	if err != nil {
		e.logger.Errorf("setHeartBeat error: %v", err)
		return
	}
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				e.logger.Errorf("keepAlive panic: %v", err)
				return
			}
		}()
		for {
			select {
			case _, ok := <-leaseCh:
				if !ok {
					e.revoke()
					e.SetHeartBeat(node)
					return
				}
			case <-time.After(businessTimeout):
				e.logger.Errorf("ectd cli keepalive timeout")
				return
			}
		}
	}()
}

func (e *EtcdDriver) SetLogger(log dlog.Logger) {
	e.logger = log
}

// SetTimeout set etcd lease timeout
func (e *EtcdDriver) SetTimeout(timeout time.Duration) {
	e.lease = int64(timeout.Seconds())
}

// GetServiceNodeList get service notes
func (e *EtcdDriver) GetServiceNodeList(serviceName string) ([]*node.Node, error) {
	services := e.getServices(serviceName)
	if len(services) == 0 {
		return []*node.Node{}, nil
	}
	result := make([]*node.Node, len(services))
	for i, service := range services {
		result[i] = &node.Node{}
		if err := result[i].UnmarshalBinary([]byte(service)); err != nil {
			return nil, fmt.Errorf("[etcd] unmarshal node error: %w, node: %s", err, service)
		}
	}
	return result, nil
}

// RegisterServiceNode register a node to service
func (e *EtcdDriver) RegisterServiceNode(node *node.Node) (string, error) {
	node.ID = e.randNodeID(node.ServiceName)
	bytes, err := node.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("[etcd] marshal node error: %w, node: %v", err, node)
	}
	_, err = e.putKeyWithLease(node.ID, string(bytes))
	if err != nil {
		return "", err
	}
	err = e.watchService(node.ServiceName)
	if err != nil {
		return "", err
	}
	return node.ID, nil
}
