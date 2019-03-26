package store

import (
	"container/list"
	"sync"

	"github.com/open-falcon/common/model"
)

// 保存item缓存数据，其作为了HistoryBigMap的值，HistoryBigMap的key为item key前面两个字母
type JudgeItemMap struct {
	sync.RWMutex
	M map[string]*SafeLinkedList
}

func NewJudgeItemMap() *JudgeItemMap {
	return &JudgeItemMap{M: make(map[string]*SafeLinkedList)}
}

// 获取
func (this *JudgeItemMap) Get(key string) (*SafeLinkedList, bool) {
	this.RLock()
	defer this.RUnlock()
	val, ok := this.M[key]
	return val, ok
}

// 设置
func (this *JudgeItemMap) Set(key string, val *SafeLinkedList) {
	this.Lock()
	defer this.Unlock()
	this.M[key] = val
}

// 长度
func (this *JudgeItemMap) Len() int {
	this.RLock()
	defer this.RUnlock()
	return len(this.M)
}

// 删除
func (this *JudgeItemMap) Delete(key string) {
	this.Lock()
	defer this.Unlock()
	delete(this.M, key)
}

// 批量删除
func (this *JudgeItemMap) BatchDelete(keys []string) {
	count := len(keys)
	if count == 0 {
		return
	}

	this.Lock()
	defer this.Unlock()
	for i := 0; i < count; i++ {
		delete(this.M, keys[i])
	}
}

// 删除before之前的数据
func (this *JudgeItemMap) CleanStale(before int64) {
	keys := []string{}

	this.RLock()
	for key, L := range this.M {
		front := L.Front()
		if front == nil {
			continue
		}

		if front.Value.(*model.JudgeItem).Timestamp < before {
			keys = append(keys, key)
		}
	}
	this.RUnlock()

	this.BatchDelete(keys)
}

// 先对合法性进行检查，然后首部插入数据，然后进行告警判断，告警event发送
func (this *JudgeItemMap) PushFrontAndMaintain(key string, val *model.JudgeItem, maxCount int, now int64) {
	if linkedList, exists := this.Get(key); exists {
		// 先对合法性进行检查，然后首部插入数据
		needJudge := linkedList.PushFrontAndMaintain(val, maxCount)
		if needJudge {
			// 告警判断，event发送
			Judge(linkedList, val, now)
		}
	} else {
		// 不存在key，则创建
		NL := list.New()
		NL.PushFront(val)
		safeList := &SafeLinkedList{L: NL}
		// 保存到JudgeItemMap中
		this.Set(key, safeList)
		Judge(safeList, val, now)
	}
}

// 这是个线程不安全的大Map，需要提前初始化好
var HistoryBigMap = make(map[string]*JudgeItemMap)

// 创建16*16=256个key的map，map值类型仍为map，值的key为item key，值的值为链表
func InitHistoryBigMap() {
	arr := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}
	for i := 0; i < 16; i++ {
		for j := 0; j < 16; j++ {
			HistoryBigMap[arr[i]+arr[j]] = NewJudgeItemMap()
		}
	}
}
