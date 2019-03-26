package rpc

import (
	"time"

	"github.com/open-falcon/common/model"
	"github.com/open-falcon/judge/g"
	"github.com/open-falcon/judge/store"
)

// 实现RpcServer处理函数
type Judge int

// 连通性检查
func (this *Judge) Ping(req model.NullRpcRequest, resp *model.SimpleRpcResponse) error {
	return nil
}

// 接收Transfer推送的数据
func (this *Judge) Send(items []*model.JudgeItem, resp *model.SimpleRpcResponse) error {
	// 读取每个item保存数据的个数
	remain := g.Config().Remain
	// 把当前时间的计算放在最外层，是为了减少获取时间时的系统调用开销
	now := time.Now().Unix()
	for _, item := range items {
		pk := item.PrimaryKey()
		// 先对合法性进行检查，然后首部插入数据，然后进行告警判断，告警event发送
		store.HistoryBigMap[pk[0:2]].PushFrontAndMaintain(pk, item, remain, now)
	}
	return nil
}
