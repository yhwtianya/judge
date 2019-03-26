package cron

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/open-falcon/common/model"
	"github.com/open-falcon/judge/g"
)

// 周期性同步Strategy和Expression
func SyncStrategies() {
	duration := time.Duration(g.Config().Hbs.Interval) * time.Second
	for {
		syncStrategies()
		syncExpression()
		time.Sleep(duration)
	}
}

// 从Hbs同步Strategy
func syncStrategies() {
	var strategiesResponse model.StrategiesResponse
	err := g.HbsClient.Call("Hbs.GetStrategies", model.NullRpcRequest{}, &strategiesResponse)
	if err != nil {
		log.Println("[ERROR] Hbs.GetStrategies:", err)
		return
	}

	// 将Hbs的策略结构转为StrategyMap结构保存
	rebuildStrategyMap(&strategiesResponse)
}

// 将Hbs的策略结构转为StrategyMap结构保存
func rebuildStrategyMap(strategiesResponse *model.StrategiesResponse) {
	// endpoint:metric => [strategy1, strategy2 ...]
	m := make(map[string][]model.Strategy)
	for _, hs := range strategiesResponse.HostStrategies {
		hostname := hs.Hostname
		if g.Config().Debug && hostname == g.Config().DebugHost {
			// 可以debug输出单台主机的策略
			log.Println(hostname, "strategies:")
			bs, _ := json.Marshal(hs.Strategies)
			fmt.Println(string(bs))
		}
		// 转为StrategyMap数据元素
		for _, strategy := range hs.Strategies {
			key := fmt.Sprintf("%s/%s", hostname, strategy.Metric)
			if _, exists := m[key]; exists {
				m[key] = append(m[key], strategy)
			} else {
				m[key] = []model.Strategy{strategy}
			}
		}
	}

	g.StrategyMap.ReInit(m)
}

// 从Hbs同步Expression
func syncExpression() {
	var expressionResponse model.ExpressionResponse
	err := g.HbsClient.Call("Hbs.GetExpressions", model.NullRpcRequest{}, &expressionResponse)
	if err != nil {
		log.Println("[ERROR] Hbs.GetExpressions:", err)
		return
	}

	rebuildExpressionMap(&expressionResponse)
}

// 将Hbs的Expression结构转为ExpressionMap结构保存
func rebuildExpressionMap(expressionResponse *model.ExpressionResponse) {
	m := make(map[string][]*model.Expression)
	for _, exp := range expressionResponse.Expressions {
		for k, v := range exp.Tags {
			key := fmt.Sprintf("%s/%s=%s", exp.Metric, k, v)
			if _, exists := m[key]; exists {
				m[key] = append(m[key], exp)
			} else {
				m[key] = []*model.Expression{exp}
			}
		}
	}

	g.ExpressionMap.ReInit(m)
}
