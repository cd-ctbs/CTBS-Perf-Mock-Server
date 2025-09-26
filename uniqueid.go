package main

//
//import (
//	"fmt"
//	"sync/atomic"
//	"time"
//)
//
//// IdGenerator 全局ID生成器结构体
//type IdGenerator struct {
//	lastTimestamp atomic.Int64
//	sequence      atomic.Int64
//}
//
//// defaultGenerator 默认的全局ID生成器实例
//var defaultGenerator = &IdGenerator{}
//
//// GenerateUniqueId 生成18位唯一ID (13位时间戳 + 5位序列号)
//// 这是包级别的全局函数，可以直接调用 uniqueid.GenerateUniqueId()
//func GenerateUniqueId() string {
//	for {
//		lastTs := defaultGenerator.lastTimestamp.Load()
//		currentTimestamp := time.Now().UnixMilli()
//
//		// 处理时钟回拨
//		if currentTimestamp < lastTs {
//			// 等待直到时间追上
//			for currentTimestamp < lastTs {
//				time.Sleep(time.Millisecond)
//				currentTimestamp = time.Now().UnixMilli()
//			}
//			continue
//		}
//
//		var seq int64
//		if currentTimestamp == lastTs {
//			// 相同时间戳，递增序列号
//			seq = defaultGenerator.sequence.Add(1)
//			// 如果序列号超过5位数限制(0-99999)
//			if seq > 99999 {
//				// 等待下一毫秒
//				for currentTimestamp <= defaultGenerator.lastTimestamp.Load() {
//					currentTimestamp = time.Now().UnixMilli()
//				}
//				// 重置序列号
//				defaultGenerator.sequence.Store(0)
//				defaultGenerator.lastTimestamp.Store(currentTimestamp)
//				seq = 0
//			}
//		} else {
//			// 新的时间戳，重置序列号
//			// 使用 Compare-And-Swap 确保原子性
//			if defaultGenerator.lastTimestamp.CompareAndSwap(lastTs, currentTimestamp) {
//				defaultGenerator.sequence.Store(0)
//				seq = 0
//			} else {
//				// 其他线程已经更新了时间戳，重新循环
//				continue
//			}
//		}
//
//		return fmt.Sprintf("%013d%05d", currentTimestamp, seq)
//	}
//}
//
//// GetGeneratorInstance 获取独立的ID生成器实例
//// 当你需要多个独立的ID生成器时使用（通常情况不需要）
//func GetGeneratorInstance() *IdGenerator {
//	return &IdGenerator{}
//}
//
//// GenerateUniqueId 为 IdGenerator 结构体定义的方法
//// 通过实例调用: generator := uniqueid.GetGeneratorInstance(); id := generator.GenerateUniqueId()
//func (g *IdGenerator) GenerateUniqueId() string {
//	for {
//		lastTs := g.lastTimestamp.Load()
//		currentTimestamp := time.Now().UnixMilli()
//
//		// 处理时钟回拨
//		if currentTimestamp < lastTs {
//			for currentTimestamp < lastTs {
//				time.Sleep(time.Millisecond)
//				currentTimestamp = time.Now().UnixMilli()
//			}
//			continue
//		}
//
//		var seq int64
//		if currentTimestamp == lastTs {
//			// 相同时间戳，递增序列号
//			seq = g.sequence.Add(1)
//			// 如果序列号超过5位数限制
//			if seq > 99999 {
//				// 等待下一毫秒
//				for currentTimestamp <= g.lastTimestamp.Load() {
//					currentTimestamp = time.Now().UnixMilli()
//				}
//				// 重置序列号
//				g.sequence.Store(0)
//				g.lastTimestamp.Store(currentTimestamp)
//				seq = 0
//			}
//		} else {
//			// 新的时间戳，重置序列号
//			g.sequence.Store(0)
//			g.lastTimestamp.Store(currentTimestamp)
//			seq = 0
//		}
//
//		return fmt.Sprintf("%013d%05d", currentTimestamp, seq)
//	}
//}
