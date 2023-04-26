package driver

import (
	"github.com/Casper-Mars/dcron/node"
	"time"

	"github.com/Casper-Mars/dcron/dlog"
)

// Driver is a driver interface
type Driver interface {
	// Ping is check dirver is valid
	Ping() error
	SetLogger(log dlog.Logger)
	SetHeartBeat(node *node.Node)
	SetTimeout(timeout time.Duration)
	GetServiceNodeList(ServiceName string) ([]*node.Node, error)
	RegisterServiceNode(node *node.Node) (string, error)
}
