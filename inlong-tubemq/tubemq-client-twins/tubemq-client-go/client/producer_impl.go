package client

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/config"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/errs"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/log"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/metadata"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/multiplexing"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/protocol"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/pub"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/rpc"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/selector"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/tdmsg"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/transport"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/util"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var as allowedSetting

// The allowedSetting struct caches the dynamic settings
// returned from the server.
type allowedSetting struct {
	configId   int64
	maxMsgSize int32
}

func init() {
	as = allowedSetting{}
}

func (a *allowedSetting) updAllowedSetting(allowedConfig *protocol.ApprovedClientConfig) {
	if allowedConfig != nil {
		if a.configId != allowedConfig.GetConfigId() {
		}
		a.configId = allowedConfig.GetConfigId()
		if allowedConfig.GetMaxMsgSize() != a.maxMsgSize {
			a.maxMsgSize = allowedConfig.GetMaxMsgSize()
		}
	}
}

type ClientAuthenticateHandler interface {
	genMasterAuthenticateToken(userName string, password string) protocol.AuthenticateInfo

	genBrokerAuthenticateToken(userName string, password string) string
}

type simpleClientAuthenticateHandler struct {
}

func (s simpleClientAuthenticateHandler) genMasterAuthenticateToken(userName string, password string) protocol.AuthenticateInfo {
	panic("implement me")
}

func (s simpleClientAuthenticateHandler) genBrokerAuthenticateToken(userName string, password string) string {
	panic("implement me")
}

type partitionRouter interface {
	getPartition(message *tdmsg.Message, partitions []*metadata.Partition) *metadata.Partition
}

type RoundRobinPartitionRouter struct {
	name string
}

func (r RoundRobinPartitionRouter) getPartition(message *tdmsg.Message, partitions []*metadata.Partition) *metadata.Partition {
	// todo
	//_ := message.Topic
	return partitions[0]
}

type ClientStatsInfo struct {
}

var producerCounter uint64

type producer struct {
	//producerCounter int64
	// 2.1
	producerId     string         // 2.2
	producerAddrId int            // 2.3
	config         *config.Config //  2.4 todo add producer config
	// 这里和java中的是实现不一样，rpc 主要负责发送和接收消息，
	// heartbeatManager主要用于心跳的管理，这里功能职责不一样。
	client           rpc.RPCClient       // 2.4 2.5 2.6 2.8
	heartbeatManager *heartbeatManager4P // 2.9 2.14
	publishTopics    sync.Map            // 2.7
	visitToken       int64               // 2.10
	allowedSetting   allowedSetting      // 2.11
	// 2.12
	simpleClientAuthenticateHandler simpleClientAuthenticateHandler          // 2.13
	brokersMap                      sync.Map                                 // 2.15
	brokerInfoCheckSum              int64                                    // 2.16
	lastBrokerUpdatedTime           time.Time                                // 2.17
	lastEmptyBrokerPrintTime        int64                                    // 2.18
	lastEmptyTopicPrintTime         int64                                    // 2.19
	heartbeatRetryTimes             int                                      // 2.20
	isStartHeart                    bool                                     // 2.21
	heartBeatStatus                 int                                      // 2.22
	lastHeartbeatTime               time.Time                                // 2.23
	nodeStatus                      int                                      // 2.24  用于确定生产者的状态。
	topicPartitionMap               map[string]map[int][]*metadata.Partition // 2.25
	nextWithAuthInfo2M              bool                                     // 2.26
	clientStatsInfo                 ClientStatsInfo                          // 2.27

	// 补充
	master          *selector.Node // master 节点信息
	partitionRouter partitionRouter

	// todo
	selector selector.Selector

	// 不确定要不要的？
	nextAuth2Master int32
	done            chan struct{}
	closeOnce       sync.Once
}

func NewProducer(config *config.Config) (Producer, error) {
	selector, err := selector.Get("ip")
	if err != nil {
		return nil, err
	}
	producerId := generateProducerID()
	pool := multiplexing.NewPool()
	opts := &transport.Options{}
	if config.Net.TLS.Enable {
		opts.TLSEnable = true
		opts.CACertFile = config.Net.TLS.CACertFile
		opts.TLSCertFile = config.Net.TLS.TLSCertFile
		opts.TLSKeyFile = config.Net.TLS.TLSKeyFile
		opts.TLSServerName = config.Net.TLS.TLSServerName
	}
	rpcClient := rpc.New(pool, opts, config)
	roundRobinPartitionRouter := &RoundRobinPartitionRouter{
		name: "RoundRobinPartitionRouter",
	}
	p := &producer{
		config:          config,
		producerId:      producerId,
		selector:        selector,
		client:          rpcClient,
		visitToken:      util.InvalidValue,
		done:            make(chan struct{}),
		partitionRouter: roundRobinPartitionRouter,
	}
	hbm4p := newHBManager4P(p)
	p.heartbeatManager = hbm4p
	err = p.register2Master(true)
	if err != nil {
		return nil, err
	}
	p.heartbeatManager.registerMaster(p.master.Address)
	// 生产者有相关的事务吗？
	log.Info("[PRODUCER] start producer success, producer client=%s", producerId)
	return p, nil
}

func (p *producer) checkServiceStatus() error {
	if p.nodeStatus < 0 { // todo
		return fmt.Errorf("status error: please call start function first")
	}
	if p.nodeStatus > 0 {
		return fmt.Errorf("status error: producer service has been shutdown")
	}
	return nil
}

func (p *producer) Publish(topic string) error {
	err := p.checkServiceStatus()
	defer func() {}() // todo
	if err != nil {
		return err
	}
	var buffer *bytes.Buffer
	buffer.WriteString("[Publish begin 1] publish topic ")
	buffer.WriteString(topic)
	buffer.WriteString(", address = ")
	//buffer.WriteString(p.String())  // todo

	log.Infof(buffer.String())

	curPubCnt, ok := p.publishTopics.Load(topic)
	if !ok {
		curPubCntInt := 0
		// 主体如果不存在就新建一个。
		p.publishTopics.Store(topic, curPubCntInt)

	}
	curPubCntInt := curPubCnt.(int32) // todo
	buffer.Reset()
	if atomic.AddInt32(&curPubCntInt, 1) == 1 {
		// 开启心跳任务 不需要，通过心跳管理器管理。
		//go p.heartbeatManager.

	}
	panic("im")
}

func (p *producer) PublishMany(topics map[string]struct{}) error {
	panic("im")
}

func (p *producer) GetPublishedTopicSet() map[string]struct{} {
	panic("im")
}

func (p *producer) IsTopicCurAcceptPublish(topic string) bool {
	panic("im")
}

func (p *producer) SendMessage(message *tdmsg.Message) (*MessageSentResult, error) {
	// todo 检查消息是否正常。
	// 1. 获取分区信息
	partition, err := p.selectPartition(message)
	if err != nil {
		return nil, err
	}

	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	// 2. 直接从分区中获取broker的信息。
	node.SetAddress(partition.GetBroker().GetAddress())
	m.SetNode(node)
	ctx, cancel := context.WithTimeout(context.Background(), p.config.Net.ReadTimeout)
	pubInfo := &pub.PubInfo{} // todo
	defer cancel()
	rsp, err := p.client.SendMessageP2B(ctx, m, pubInfo, partition, message)
	if err != nil {
		log.Infof("[PRODUCER]GetMessage error %s", err.Error())
		return nil, err
	}
	// processSendMessageRspB2P 中处理并构建返回结果
	msg := p.processSendMessageRspB2P(rsp, partition, message)
	return msg, nil
}

func (p *producer) processSendMessageRspB2P(b2P *protocol.SendMessageResponseB2P, partition *metadata.Partition,
	message *tdmsg.Message) *MessageSentResult {
	panic("im")
}

// 将Create这个请求，放入到 rpc client中。
//func (p *producer) createSendMessageRequest(partition *metadata.Partition, message Message) protocol.SendMessageRequestP2B {
//	builder := protocol.SendMessageRequestP2B{}
//	builder.ClientId = &p.producerId
//	topicName := partition.GetTopic()  // 为啥这样可以？ &partition.GetTopic()  就不行
//	builder.TopicName = &topicName
//	builder.PartitionId = partition.GetPartitionID()
//}

func (p *producer) SendMessageAsync(message *tdmsg.Message, t func(*MessageSentResult, error)) {
	panic("implement me")
}

func (p *producer) Close() {
	p.closeOnce.Do(func() {
		log.Infof("[PRODUCER]Begin to close consumer, client=%s", p.producerId)
		close(p.done)
		p.heartbeatManager.close()
		p.close2Master()
		p.closeAllBrokers()
		p.client.Close()
		log.Infof("[PRODUCER]Consumer has been closed successfully, client=%s", p.producerId)
	})
}

func (p *producer) selectPartition(message *tdmsg.Message) (*metadata.Partition, error) {
	var buffer bytes.Buffer
	topic := message.Topic
	//1. 从ProducerManager中返回全部分区，
	brokerTopicPartList := p.getTopicPartition(topic)
	if brokerTopicPartList == nil || len(brokerTopicPartList) == 0 {
		buffer.WriteString("Null partition for topic: ")
		buffer.WriteString(topic)
		buffer.WriteString(", please try later!")
		return nil, fmt.Errorf(buffer.String())
	}
	// 通信质量控制，请求允许的broker
	// 2.返回允许的broker分区。
	partitionsSlice, err := p.getAllowedBrokerPartitions(brokerTopicPartList)
	if err != nil {
		return nil, err
	}
	// 3. 返回此条消息发送的分区。
	// PartitionRouter 可以做一些 分区负载均衡的事情，因为是router嘛
	partition := p.partitionRouter.getPartition(message, partitionsSlice)
	buffer.Reset()
	if partition == nil {
		buffer.WriteString("Not found available partition for topic: ")
		buffer.WriteString(message.Topic)
		return partition, errs.New(-1, buffer.String())
	}
	// 检查rpc服务是否正常 todo
	//broker := partition.GetBroker()
	// 创建rpc 服务吗？
	return partition, nil
}

func (p *producer) getAllowedBrokerPartitions(topicMap map[int][]*metadata.Partition) (
	[]*metadata.Partition, error) {
	//var partSlice []*metadata.Partition =
	//partSlice := make([]*metadata.Partition, 16) // todo
	if topicMap == nil || len(topicMap) == 0 {
		return nil, errs.New(-1, "nil brokers to select sent, please try later!")
	}
	// todo  more process logic

	return topicMap[1], nil
}

func (p *producer) getTopicPartition(topic string) map[int][]*metadata.Partition {
	m := p.topicPartitionMap[topic]
	return m
}

func generateProducerID() string {
	pid := os.Getegid()
	var buffer bytes.Buffer
	buffer.WriteString(util.GetLocalHost())
	buffer.WriteString("-")
	buffer.WriteString(string(rune(pid)))
	buffer.WriteString("-")
	buffer.WriteString(strconv.FormatInt(time.Now().Unix(), 10))
	buffer.WriteString("-")
	buffer.WriteString(strconv.Itoa(int(atomic.AddUint64(&producerCounter, 1))))
	buffer.WriteString("-go-")
	buffer.WriteString(tubeMQClientVersion)
	return buffer.String()
}

func (p *producer) register2Master(needChange bool) error {
	if needChange {
		node, err := p.selector.Select(p.config.Producer.Masters)
		if err != nil {
			return err
		}
		p.master = node
	}
	retryCount := 0
	for {
		rsp, err := p.sendRegRequest2Master()
		if err != nil || !rsp.GetSuccess() {
			if err != nil {
				log.Errorf("[PRODUCER] register2Master error %s", err.Error())
			} else if rsp.GetErrCode() == errs.RetErrHBNoNode {

			}
			if !p.master.HasNext {
				if err != nil {
					return err
				}
				if rsp != nil {
					log.Errorf("[PRODUCER] register2master(%s) failure exist register, producer=%s, error: %s",
						p.master.Address, p.producerId, rsp.GetErrMsg())
				}
				break
			}
			retryCount++
			retryCount++
			log.Warnf("[CONSUMER] register2master(%s) failure, client=%s, retry count=%d",
				p.master.Address, p.producerId, retryCount)
			// 选择另外的Master
			if p.master, err = p.selector.Select(p.config.Producer.Masters); err != nil {
				return err
			}
			continue

		}
		log.Infof("[PRODUCER] register2Master response %s", rsp.String())
		p.heartbeatRetryTimes = 0
		p.processRegisterResponseM2P(rsp)
		break
	}
	return nil
}

func (p *producer) sendRegRequest2Master() (*protocol.RegisterResponseM2P, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.Net.ReadTimeout)
	defer cancel()
	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	node.SetAddress(p.master.Address)
	auth := &protocol.AuthenticateInfo{}
	if p.needGenMasterCertificateInfo(true) {
		util.GenMasterAuthenticateToken(auth, p.config.Net.Auth.UserName, p.config.Net.Auth.Password)
	}
	m.SetNode(node)
	rsp, err := p.client.RegisterRequestP2M(ctx, m) // todo 这里的入参还需要继续判定
	return rsp, err
}

func (p *producer) needGenMasterCertificateInfo(force bool) bool {
	needAdd := false
	if p.config.Net.Auth.Enable {
		if force {
			needAdd = true
			atomic.StoreInt32(&p.nextAuth2Master, 0)
		} else if atomic.LoadInt32(&p.nextAuth2Master) == 1 {
			if atomic.CompareAndSwapInt32(&p.nextAuth2Master, 1, 0) {
				needAdd = true
			}
		}
		if needAdd {
		}
	}
	return needAdd
}

// todo
func (p *producer) processRegisterResponseM2P(rsp *protocol.RegisterResponseM2P) {
	if rsp.GetAuthorizedInfo() != nil {

	} else {

	}
}

func (p *producer) processAuthorizedToken(info *protocol.MasterAuthorizedInfo) {
	if info != nil {
		p.visitToken = info.GetVisitAuthorizedToken()
		if info.GetAuthAuthorizedToken() != "" {
			// todo
		}
	}
}

func (p *producer) procAllowedConfig4P(config *protocol.ApprovedClientConfig) {
	if config != nil {
		as.updAllowedSetting(config) // todo 包级的变量怎么处理。
	}

}

func (p *producer) close2Master() {
	log.Infof("[PRODUCER] close2Master begin, client=%s", p.producerId)
	ctx, cancel := context.WithTimeout(context.Background(), p.config.Net.ReadTimeout)
	defer cancel()

	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	node.SetAddress(p.master.Address)
	m.SetNode(node)
	//sub := &metadata.SubscribeInfo{}
	//sub.SetGroup(p.config.Consumer.Group)
	//m.SetSubscribeInfo(sub)
	//mci := &protocol.MasterCertificateInfo{}
	auth := &protocol.AuthenticateInfo{}
	if p.needGenMasterCertificateInfo(true) {
		util.GenMasterAuthenticateToken(auth, p.config.Net.Auth.UserName, p.config.Net.Auth.Password)
	}
	//p.subInfo.SetMasterCertificateInfo(mci)
	rsp, err := p.client.CloseRequestP2M(ctx, m)
	if err != nil {
		log.Errorf("[PRODUCER] fail to close master, error: %s", err.Error())
		return
	}
	if !rsp.GetSuccess() {
		log.Errorf("[CONSUMER] fail to close master, error code: %d, error msg: %s",
			rsp.GetErrCode(), rsp.GetErrMsg())
		return
	}
	log.Infof("[CONSUMER] close2Master finished, client=%s", p.producerId)
}

func (p *producer) closeAllBrokers() {
	log.Infof("[CONSUMER] closeAllBrokers begin, client=%s", p.producerId)
	//partitions := p.rmtDataCache.GetAllClosedBrokerParts()
	//if len(partitions) > 0 {
	//	p.unregister2Broker(partitions)
	//}
	log.Infof("[CONSUMER] closeAllBrokers end, client=%s", p.producerId)
}

// 这一块的实现，要不要放到心跳管理里面 ，因为心跳管理里面就不停的在执行。
func (p *producer) ProducerHeartbeatTask() {
	// 更新话题分区。
	//topicPartitionMap  // todo

	// check whether public topics exist
	topicCnt := 0
	p.publishTopics.Range(func(key, value interface{}) bool {
		topicCnt++
		return true
	})
	if topicCnt == 0 {
		return
	}
}

func (p *producer) GetClientID() string {
	panic("im")
}
