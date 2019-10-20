package mqclient

import (
	"strconv"
	"time"
)

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

type defaultPullCallback struct {
}

func (cb *defaultPullCallback) OnSuccess(result PullResult) {

}

func (cb *defaultPullCallback) OnError(err error) {

}

func resolvePullResult(result *PullResult) {
	if result.PullStatus == PullFound {
		msgList := decodeMessage(result.messageBinary)
		for _, msgExt := range msgList {
			traFlag := msgExt.GetProperty(MessageTransactionPrepared)
			if res, err := strconv.ParseBool(traFlag); err == nil && res {
				msgExt.TransactionID = msgExt.GetProperty(MessageUniqClientMessageIDKeyIdx)
			}
			msgExt.SetProperty(MessageMinOffset, strconv.FormatInt(result.MinOffset, 10))
			msgExt.SetProperty(MessageMaxOffset, strconv.FormatInt(result.MaxOffset, 10))
		}

		result.MsgFoundList = msgList
	}
	result.messageBinary = nil
}
