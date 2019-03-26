package g

import (
	"sync"
	"time"

	"github.com/open-falcon/common/model"
)

type SafeStrategyMap struct {
	sync.RWMutex
	// endpoint:metric => [strategy1, strategy2 ...]
	M map[string][]model.Strategy
}

type SafeExpressionMap struct {
	sync.RWMutex
	// metric:tag1 => [exp1, exp2 ...]
	// metric:tag2 => [exp1, exp2 ...]
	M map[string][]*model.Expression
}

type SafeEventMap struct {
	sync.RWMutex
	M map[string]*model.Event
}

var (
	// Hbs Rpc客户端
	HbsClient *SingleConnRpcClient
	// 保存所有策略，key为Endpoint/Metric
	StrategyMap = &SafeStrategyMap{M: make(map[string][]model.Strategy)}
	// 保存所有Expression，key为Metric/tag1=val1或者Metric/Endpoint=end。Expression是在Metric级别进行告警判断，Endpoint可以作为其tag进行过滤
	// Metric上可以有多个tag，即一个Metric上有多个key。即Expression.Tags被扁平化，每个tag和Metric一起构成了多个key。即多个key会指向同一个Expression
	ExpressionMap = &SafeExpressionMap{M: make(map[string][]*model.Expression)}
	// 保存最新event，key为event id, event id由strategy(或expression).Id和item key构成
	LastEvents = &SafeEventMap{M: make(map[string]*model.Event)}
)

// 初始化Hbs的Rpc客户端
func InitHbsClient() {
	HbsClient = &SingleConnRpcClient{
		RpcServers: Config().Hbs.Servers,
		Timeout:    time.Duration(Config().Hbs.Timeout) * time.Millisecond,
	}
}

// 重置SafeStrategyMap
func (this *SafeStrategyMap) ReInit(m map[string][]model.Strategy) {
	this.Lock()
	defer this.Unlock()
	this.M = m
}

//  获取Strategy
func (this *SafeStrategyMap) Get() map[string][]model.Strategy {
	this.RLock()
	defer this.RUnlock()
	return this.M
}

// 重置SafeExpressionMap
func (this *SafeExpressionMap) ReInit(m map[string][]*model.Expression) {
	this.Lock()
	defer this.Unlock()
	this.M = m
}

// 获取Expression
func (this *SafeExpressionMap) Get() map[string][]*model.Expression {
	this.RLock()
	defer this.RUnlock()
	return this.M
}

// 根据event id获取最新event
func (this *SafeEventMap) Get(key string) (*model.Event, bool) {
	this.RLock()
	defer this.RUnlock()
	event, exists := this.M[key]
	return event, exists
}

// 更新item最新event
func (this *SafeEventMap) Set(key string, event *model.Event) {
	this.Lock()
	defer this.Unlock()
	this.M[key] = event
}
