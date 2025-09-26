package main

import (
	"fmt"
	"sync"
	"time"
)

// IdGenerator 全局ID生成器结构体 (雪花算法版本)
type IdGenerator struct {
	workerId      int64      // 工作机器ID (2位: 0-99)
	lastTimestamp int64      // 上次生成ID的时间戳
	sequence      int64      // 序列号 (3位: 0-999)
	lock          sync.Mutex // 互斥锁，确保线程安全
}

// defaultGenerator 默认的全局ID生成器实例
var defaultGenerator = &IdGenerator{
	workerId: 10, // 默认工作机器ID
}

// EPOCH 起始时间戳 (2024-01-01 00:00:00 UTC)
const EPOCH = 1704067200000

// GenerateUniqueId 生成18位唯一ID (13位时间戳 + 2位机器ID + 3位序列号)
// 这是包级别的全局函数，可以直接调用 uniqueid.GenerateUniqueId()
func GenerateUniqueId() string {
	return defaultGenerator.GenerateUniqueId()
}

// GetGeneratorInstance 获取独立的ID生成器实例
// 当你需要多个独立的ID生成器时使用（通常情况不需要）
func GetGeneratorInstance() *IdGenerator {
	return &IdGenerator{
		workerId: 1, // 可以根据需要设置不同的workerId
	}
}

// GenerateUniqueId 为 IdGenerator 结构体定义的方法
// 通过实例调用: generator := uniqueid.GetGeneratorInstance(); id := generator.GenerateUniqueId()
func (g *IdGenerator) GenerateUniqueId() string {
	g.lock.Lock()
	defer g.lock.Unlock()

	// 获取当前时间戳
	timestamp := time.Now().UnixMilli()

	// 处理时钟回拨
	if timestamp < g.lastTimestamp {
		// 等待时钟追上
		for timestamp < g.lastTimestamp {
			time.Sleep(time.Millisecond)
			timestamp = time.Now().UnixMilli()
		}
	}

	// 生成序列号
	var sequence int64
	if timestamp == g.lastTimestamp {
		// 同一毫秒内，序列号递增
		g.sequence = (g.sequence + 1) % 1000 // 3位序列号 (0-999)
		sequence = g.sequence

		// 如果序列号用完，等待下一毫秒
		if sequence == 0 {
			for timestamp <= g.lastTimestamp {
				timestamp = time.Now().UnixMilli()
			}
		}
	} else {
		// 新的毫秒，重置序列号
		g.sequence = 0
		sequence = 0
	}

	// 更新上次时间戳
	g.lastTimestamp = timestamp

	// 构造18位ID: 时间戳(13位) + 机器ID(2位) + 序列号(3位)
	timestampPart := timestamp - EPOCH // 相对于EPOCH的时间戳
	return fmt.Sprintf("%013d%02d%03d", timestampPart, g.workerId, sequence)
}

// SetWorkerId 设置工作机器ID (用于分布式场景)
func (g *IdGenerator) SetWorkerId(workerId int64) {
	g.lock.Lock()
	defer g.lock.Unlock()

	if workerId < 0 || workerId > 99 {
		panic("Worker ID must be between 0 and 99")
	}
	g.workerId = workerId
}
