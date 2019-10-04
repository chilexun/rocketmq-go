package mqclient

type consumeProcess struct {
}

type consumeRequest struct {
	messages []MessageExt
	queue    MessageQueue
}

func (p *consumeProcess) process(msgs []MessageExt, mq MessageQueue) {

}
