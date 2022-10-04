package client

import (
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/metadata"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/protocol"
	"sync"
	"time"
)

type heartbeatManager4P struct {
	producer   *producer
	heartbeats map[string]*heartbeatMetadata
	mu         sync.Mutex
}

// 构建生产者的心跳管理器
func newHBManager4P(producer *producer) *heartbeatManager4P {
	//
	return &heartbeatManager4P{
		producer:   producer,
		heartbeats: make(map[string]*heartbeatMetadata),
	}
}

func (h *heartbeatManager4P) registerMaster(address string) {

}

func (h *heartbeatManager4P) resetMasterHeartbeat() {
	h.mu.Lock()
	defer h.mu.Unlock()
	hm := h.heartbeats[h.producer.master.Address]
	// 设置下次心跳的时间。
	hm.timer.Reset(h.nextHeartbeatInterval())
}

func (h *heartbeatManager4P) nextHeartbeatInterval() time.Duration {
	// 心跳时间间隔。
	interval := h.producer.config.Heartbeat.Interval
	// 这里需要确认 heartbeatRetryTimes 的含义
	if h.producer.heartbeatRetryTimes >= h.producer.config.Heartbeat.MaxRetryTimes {
		interval = h.producer.config.Heartbeat.AfterFail
	}
	return interval
}

func (h *heartbeatManager4P) producerHB2Master() {

}

func (h *heartbeatManager4P) sendHeartbeatC2M(m *metadata.Metadata) (*protocol.HeartResponseM2C, error) {
	panic("implement me!")
}

func (h *heartbeatManager4P) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	//
	for _, heartbeat := range h.heartbeats {
		if !heartbeat.timer.Stop() {
			<-heartbeat.timer.C
		}
		heartbeat.timer = nil
	}
	h.heartbeats = nil
}
