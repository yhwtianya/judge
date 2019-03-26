package store

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/open-falcon/common/model"
	"github.com/open-falcon/judge/g"
)

// 进行策略匹配，告警event发送
func Judge(L *SafeLinkedList, firstItem *model.JudgeItem, now int64) {
	CheckStrategy(L, firstItem, now)
	CheckExpression(L, firstItem, now)
}

// 判断最新数据是否触发策略，是否发送event
func CheckStrategy(L *SafeLinkedList, firstItem *model.JudgeItem, now int64) {
	// 根据Endpoint和Metric可快速获取对应的策略列表
	key := fmt.Sprintf("%s/%s", firstItem.Endpoint, firstItem.Metric)
	strategyMap := g.StrategyMap.Get()
	strategies, exists := strategyMap[key]
	if !exists {
		return
	}

	for _, s := range strategies {
		// 因为key仅仅是endpoint和metric，所以得到的strategies并不一定是与当前judgeItem相关的
		// 比如lg-dinp-docker01.bj配置了两个proc.num的策略，一个name=docker，一个name=agent
		// 所以此处要排除掉一部分
		related := true
		for tagKey, tagVal := range s.Tags {
			// first必须符合strategy中的所有tag，first可以比strategy的tag多
			if myVal, exists := firstItem.Tags[tagKey]; !exists || myVal != tagVal {
				related = false
				break
			}
		}

		if !related {
			continue
		}

		// 判断是否触发策略,并判断是否发送event
		judgeItemWithStrategy(L, s, firstItem, now)
	}
}

// 判断是否触发策略,并判断是否发送event
func judgeItemWithStrategy(L *SafeLinkedList, strategy model.Strategy, firstItem *model.JudgeItem, now int64) {
	fn, err := ParseFuncFromString(strategy.Func, strategy.Operator, strategy.RightValue)
	if err != nil {
		log.Printf("[ERROR] parse func %s fail: %v. strategy id: %d", strategy.Func, err, strategy.Id)
		return
	}

	// historyData内保存了limit个历史数据
	historyData, leftValue, isTriggered, isEnough := fn.Compute(L)
	if !isEnough {
		return
	}

	event := &model.Event{
		Id:         fmt.Sprintf("s_%d_%s", strategy.Id, firstItem.PrimaryKey()), //event id由strategy.Id和item key构成
		Strategy:   &strategy,
		Endpoint:   firstItem.Endpoint,
		LeftValue:  leftValue,
		EventTime:  firstItem.Timestamp,
		PushedTags: firstItem.Tags,
	}

	// 判断是否发送Problem或OK报警
	sendEventIfNeed(historyData, isTriggered, now, event, strategy.MaxStep)
}

// 发送event到redis
func sendEvent(event *model.Event) {
	// update last event
	g.LastEvents.Set(event.Id, event)

	bs, err := json.Marshal(event)
	if err != nil {
		log.Printf("json marshal event %v fail: %v", event, err)
		return
	}

	// 每个告警级别一个队列
	// send to redis
	redisKey := fmt.Sprintf(g.Config().Alarm.QueuePattern, event.Priority())
	rc := g.RedisConnPool.Get()
	defer rc.Close()
	rc.Do("LPUSH", redisKey, string(bs))
}

func CheckExpression(L *SafeLinkedList, firstItem *model.JudgeItem, now int64) {
	// 根据JudgeItem生成可能的key
	keys := buildKeysFromMetricAndTags(firstItem)
	if len(keys) == 0 {
		return
	}

	// expression可能会被多次重复处理，用此数据结构保证只被处理一次
	handledExpression := make(map[int]struct{})

	expressionMap := g.ExpressionMap.Get()
	for _, key := range keys {
		// 根据key获取expression列表，key为Metric/tag1=val1或者Metric/Endpoint=end，不同的key会匹配到相同的expression
		expressions, exists := expressionMap[key]
		if !exists {
			continue
		}

		// 过滤出firstItem符合每个expression所有tag的expression
		related := filterRelatedExpressions(expressions, firstItem)
		for _, exp := range related {
			if _, ok := handledExpression[exp.Id]; ok {
				// 已经有key指向此expression
				continue
			}
			handledExpression[exp.Id] = struct{}{}
			judgeItemWithExpression(L, exp, firstItem, now)
		}
	}
}

// 根据JudgeItem生成可能的key
func buildKeysFromMetricAndTags(item *model.JudgeItem) (keys []string) {
	for k, v := range item.Tags {
		// 每个tag生成一个key
		keys = append(keys, fmt.Sprintf("%s/%s=%s", item.Metric, k, v))
	}
	keys = append(keys, fmt.Sprintf("%s/endpoint=%s", item.Metric, item.Endpoint))
	return
}

// 过滤出符合expression tag的expression
func filterRelatedExpressions(expressions []*model.Expression, firstItem *model.JudgeItem) []*model.Expression {
	size := len(expressions)
	if size == 0 {
		return []*model.Expression{}
	}

	exps := make([]*model.Expression, 0, size)

	for _, exp := range expressions {

		related := true

		itemTagsCopy := firstItem.Tags
		// 注意：exp.Tags 中可能会有一个endpoint=xxx的tag
		if _, ok := exp.Tags["endpoint"]; ok {
			// 需要匹配endpoint,将firstItem的endpoint提取出来，作为tag保存到itemTagsCopy
			itemTagsCopy = copyItemTags(firstItem)
		}

		for tagKey, tagVal := range exp.Tags {
			// expression的tag必须全在firstItem才行，first的tag可以更多
			if myVal, exists := itemTagsCopy[tagKey]; !exists || myVal != tagVal {
				related = false
				break
			}
		}

		if !related {
			continue
		}

		exps = append(exps, exp)
	}

	return exps
}

// 复制tags，将endpoint加入tags
func copyItemTags(item *model.JudgeItem) map[string]string {
	ret := make(map[string]string)
	ret["endpoint"] = item.Endpoint
	if item.Tags != nil && len(item.Tags) > 0 {
		for k, v := range item.Tags {
			ret[k] = v
		}
	}
	return ret
}

// 判断是否触发expression,并判断是否发送event
func judgeItemWithExpression(L *SafeLinkedList, expression *model.Expression, firstItem *model.JudgeItem, now int64) {
	fn, err := ParseFuncFromString(expression.Func, expression.Operator, expression.RightValue)
	if err != nil {
		log.Printf("[ERROR] parse func %s fail: %v. expression id: %d", expression.Func, err, expression.Id)
		return
	}

	historyData, leftValue, isTriggered, isEnough := fn.Compute(L)
	if !isEnough {
		return
	}

	event := &model.Event{
		Id:         fmt.Sprintf("e_%d_%s", expression.Id, firstItem.PrimaryKey()), //event id由expression.Id和item key构成
		Expression: expression,
		Endpoint:   firstItem.Endpoint,
		LeftValue:  leftValue,
		EventTime:  firstItem.Timestamp,
		PushedTags: firstItem.Tags,
	}

	sendEventIfNeed(historyData, isTriggered, now, event, expression.MaxStep)

}

// 判断是否发送Problem或OK报警
func sendEventIfNeed(historyData []*model.HistoryData, isTriggered bool, now int64, event *model.Event, maxStep int) {
	lastEvent, exists := g.LastEvents.Get(event.Id)
	if isTriggered {
		event.Status = "PROBLEM"
		if !exists || lastEvent.Status[0] == 'O' {
			// 本次触发了阈值，之前又没报过警，得产生一个报警Event
			event.CurrentStep = 1

			// 但是有些用户把最大报警次数配置成了0，相当于屏蔽了，要检查一下
			if maxStep == 0 {
				return
			}

			sendEvent(event)
			return
		}

		// 逻辑走到这里，说明之前Event是PROBLEM状态
		if lastEvent.CurrentStep >= maxStep {
			// 报警次数已经足够多，到达了最多报警次数了，不再报警
			return
		}

		// 这里确保limit个数据之后才再次报警，比如max(#3) > 3,历史值为2、4、4，那么第一个4第一次触发报警，
		// 最后这个4不会报警，需要到历史数据为2、4、4、5、6,即第一个报警的4不在#3的位置时才再次触发，
		// 这时记录的EventTime为最后6出现的时间，所以需要再等三个数据才会触发第三次报警
		if historyData[len(historyData)-1].Timestamp <= lastEvent.EventTime {
			// 产生过报警的点，就不能再使用来判断了，否则容易出现一分钟报一次的情况
			// 只需要拿最后一个historyData来做判断即可，因为它的时间最老
			return
		}

		// 最小报警间隔，取limit*step和MinInterval的较大值
		if now-lastEvent.EventTime < g.Config().Alarm.MinInterval {
			// 报警不能太频繁，两次报警之间至少要间隔MinInterval秒，否则就不能报警
			return
		}

		event.CurrentStep = lastEvent.CurrentStep + 1
		sendEvent(event)
	} else {
		// 如果LastEvent是Problem，报OK，否则啥都不做
		if exists && lastEvent.Status[0] == 'P' {
			event.Status = "OK"
			event.CurrentStep = 1
			sendEvent(event)
		}
	}
}
