package pub

import (
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/protocol"
	"sync/atomic"
)

// InValidOffset represents the offset which is invalid.
const InValidOffset = -2

type PubInfo struct {
	clientID              string
	boundConsume          bool
	selectBig             bool
	sourceCount           int32
	sessionKey            string
	notAllocated          int32
	firstRegistered       int32
	subscribedTime        int64
	boundPartitions       string
	topic                 string // todo    // 主题。
	topicConds            []string
	topicFilter           map[string]bool // 应该是判断是否存在过滤
	assignedPartitions    map[string]int64
	topicFilters          map[string][]string // 主题过滤
	authInfo              *protocol.AuthorizedInfo
	masterCertificateInfo *protocol.MasterCertificateInfo
}

// GetClientID returns the client ID.
func (p *PubInfo) GetClientID() string {
	return p.clientID
}

func (p *PubInfo) GetPartitionId() int32 {
	panic("im")
}

// IsFiltered returns whether a topic if filtered.
func (p *PubInfo) IsFiltered(topic string) bool {
	if filtered, ok := p.topicFilter[topic]; ok {
		return filtered
	}
	return false
}

// GetTopicFilters returns the topic filters.
func (p *PubInfo) GetTopicFilters() map[string][]string {
	return p.topicFilters
}

// GetAssignedPartOffset returns the assignedPartOffset of the given partitionKey.
func (p *PubInfo) GetAssignedPartOffset(partitionKey string) int64 {
	if p.isFirstRegistered() && p.boundConsume && p.IsNotAllocated() {
		if offset, ok := p.assignedPartitions[partitionKey]; ok {
			return offset
		}
	}
	return InValidOffset
}

// BoundConsume returns whether it is bondConsume.
func (p *PubInfo) BoundConsume() bool {
	return p.boundConsume
}

// GetSubscribedTime returns the subscribedTime.
func (p *PubInfo) GetSubscribedTime() int64 {
	return p.subscribedTime
}

// GetTopics returns the topics.
func (p *PubInfo) GetTopics() string {
	return p.topic
}

// GetTopicConds returns the topicConds.
func (p *PubInfo) GetTopicConds() []string {
	return p.topicConds
}

// GetSessionKey returns the sessionKey.
func (p *PubInfo) GetSessionKey() string {
	return p.sessionKey
}

// SelectBig returns whether it is selectBig.
func (p *PubInfo) SelectBig() bool {
	return p.selectBig
}

// GetSourceCount returns the sourceCount.
func (p *PubInfo) GetSourceCount() int32 {
	return p.sourceCount
}

// GetBoundPartInfo returns the boundPartitions.
func (p *PubInfo) GetBoundPartInfo() string {
	return p.boundPartitions
}

// IsNotAllocated returns whether it is not allocated.
func (p *PubInfo) IsNotAllocated() bool {
	return atomic.LoadInt32(&p.notAllocated) == 1
}

// GetAuthorizedInfo returns the authInfo.
func (p *PubInfo) GetAuthorizedInfo() *protocol.AuthorizedInfo {
	return p.authInfo
}

// GetMasterCertificateInfo returns the masterCertificateInfo.
func (p *PubInfo) GetMasterCertificateInfo() *protocol.MasterCertificateInfo {
	return p.masterCertificateInfo
}

// SetNotFirstRegistered sets the firstRegistered to false.
func (p *PubInfo) SetNotFirstRegistered() {
	atomic.StoreInt32(&p.firstRegistered, 0)
}

// SetAuthorizedInfo sets the authorizedInfo.
func (p *PubInfo) SetAuthorizedInfo(auth *protocol.AuthorizedInfo) {
	p.authInfo = auth
}

// SetMasterCertificateInfo sets the masterCertificateInfo.
func (p *PubInfo) SetMasterCertificateInfo(info *protocol.MasterCertificateInfo) {
	p.masterCertificateInfo = info
}

// CASIsNotAllocated sets the notAllocated.
func (p *PubInfo) CASIsNotAllocated(expected int32, update int32) {
	atomic.CompareAndSwapInt32(&p.notAllocated, expected, update)
}

// SetClientID sets the clientID.
func (p *PubInfo) SetClientID(clientID string) {
	p.clientID = clientID
}

func (p *PubInfo) isFirstRegistered() bool {
	return atomic.LoadInt32(&p.firstRegistered) == 1
}
