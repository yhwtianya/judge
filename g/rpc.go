package g

import (
	"log"
	"math"
	"net/rpc"
	"sync"
	"time"

	"github.com/toolkits/net"
)

// 封装RpcClient
type SingleConnRpcClient struct {
	sync.Mutex
	rpcClient  *rpc.Client
	RpcServers []string // rpc server地址，可以多个
	Timeout    time.Duration
}

func (this *SingleConnRpcClient) close() {
	if this.rpcClient != nil {
		this.rpcClient.Close()
		this.rpcClient = nil
	}
}

// 确保有一个rpc连接
func (this *SingleConnRpcClient) insureConn() {
	if this.rpcClient != nil {
		return
	}

	var err error
	var retry int = 1

	for {
		if this.rpcClient != nil {
			return
		}

		for _, s := range this.RpcServers {
			//获得JsonRpcClient
			this.rpcClient, err = net.JsonRpcClient("tcp", s, this.Timeout)
			if err == nil {
				return
			}

			log.Printf("dial %s fail: %s", s, err)
		}

		if retry > 6 {
			retry = 1
		}

		time.Sleep(time.Duration(math.Pow(2.0, float64(retry))) * time.Second)

		retry++
	}
}

// 通过rpc进行调用
func (this *SingleConnRpcClient) Call(method string, args interface{}, reply interface{}) error {

	this.Lock()
	defer this.Unlock()

	this.insureConn()

	err := this.rpcClient.Call(method, args, reply)
	if err != nil {
		this.close()
	}

	return err
}
