package mqclient

import "time"

type MessagePullParams struct {
	consumerGroup              string
	mq                         MessageQueue
	subExpression              string
	expressionType             ExpressionType
	subVersion                 int64
	offset                     int64
	maxNums                    int
	sysFlag                    int
	commitOffset               int64
	brokerSuspendMaxTimeMillis int64
}

type MessagePullExecutor struct {
	consumer Consumer
	mqclient *MQClient
}

func (p *MessagePullExecutor) Pull(request *MessagePullParams) {
	// go p.consumer.pullMessage(request.mq, request.offset)
}

func (p *MessagePullExecutor) pullKernelImpl(params MessagePullParams,
	timeout time.Duration, callback PullCallback) error {
	var addr string
	req := &PullMessageRequest{
		ConsumerGroup: params.consumerGroup,
		Topic:         params.mq.Topic,
		QueueID:       params.mq.QueueID,
		QueueOffset:   params.offset,
		MaxMsgNums:    params.maxNums,
	}
	p.mqclient.PullMessageAsyncRequest(addr, req, callback, timeout)
	return nil
}
