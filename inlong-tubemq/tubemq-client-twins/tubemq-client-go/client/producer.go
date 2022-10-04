package client

import (
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/metadata"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/tdmsg"
)

type MessageSentResult struct {
	Success      bool
	ErrCode      int
	ErrMsg       string
	Message      *tdmsg.Message
	MessageId    int32
	Partition    *metadata.Partition
	AppendTime   int64 // todo
	AppendOffset int64
}

type Producer interface {

	// get metadata
	// 不需要pulish，还是要呢？ 我推荐是不到的。
	Publish(topic string) error

	PublishMany(topics map[string]struct{}) error

	GetPublishedTopicSet() map[string]struct{}

	IsTopicCurAcceptPublish(topic string) bool

	// SendMessage send message
	SendMessage(message *tdmsg.Message) (*MessageSentResult, error)

	// SendMessageAsync send message async with callback func
	SendMessageAsync(message *tdmsg.Message, callback func(m *MessageSentResult, e error))

	// Close closes the producer and release the resources
	Close()

	// GetClientID returns the clientID of the consumer.
	GetClientID() string
}
