package cron

import (
	"time"

	"github.com/open-falcon/judge/store"
)

// 周期性清理7天前的数据，比如监测点或主机被删除，释放内存
func CleanStale() {
	for {
		time.Sleep(time.Hour * 5)
		cleanStale()
	}
}

// 清理7天前的数据，比如监测点或主机被删除，释放内存
func cleanStale() {
	before := time.Now().Unix() - 3600*24*7

	arr := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}
	for i := 0; i < 16; i++ {
		for j := 0; j < 16; j++ {
			store.HistoryBigMap[arr[i]+arr[j]].CleanStale(before)
		}
	}
}
