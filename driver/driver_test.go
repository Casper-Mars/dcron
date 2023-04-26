package driver_test

import (
	"flag"
	"github.com/Casper-Mars/dcron/node"
	"testing"
	"time"

	DcronDriver "github.com/Casper-Mars/dcron/driver"
	RedisDriver "github.com/Casper-Mars/dcron/driver/redis"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

var (
	redisAddr = flag.String("rAddr", "127.0.0.1:6379", "redis serve addr")
	password  = flag.String("password", "", "redis password")
)

// you should run this test when the redis is served.
// you can run test like below command.
// go test -v --rAddr 127.0.0.1:6379 -password 123456
// rAddr is the redis serve addr

func TestRedisDriver(t *testing.T) {
	t.Logf("test redis serve on %s", *redisAddr)

	serviceName := t.Name()
	NewDriverFunc := func(_ int) (DcronDriver.Driver, error) {
		driver, err := RedisDriver.NewDriver(&redis.Options{
			Addr: "127.0.0.1:6379",
		})
		require.Nil(t, err)
		require.Nil(t, driver.Ping())
		driver.SetTimeout(5 * time.Second)
		newNode := &node.Node{
			ServiceName: serviceName,
		}
		_, err = driver.RegisterServiceNode(newNode)
		require.Nil(t, err)
		driver.SetHeartBeat(newNode)
		return driver, nil
	}
	n := 10
	drivers := make([]DcronDriver.Driver, 0)
	for i := 0; i < n; i++ {
		dr, err := NewDriverFunc(i)
		require.Nilf(t, err, "new driver error %d", i)
		drivers = append(drivers, dr)
	}

	for i := 0; i < n; i++ {
		nodeIds, err := drivers[i].GetServiceNodeList(serviceName)
		require.Nilf(t, err, "get service nodelist error %d", i)
		require.Equal(t, n, len(nodeIds))
	}
}
