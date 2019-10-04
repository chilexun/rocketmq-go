package mqclient

type OffsetStore interface {
	updateOffset(mq MessageQueue, offset int64, increaseOnly bool) error
	readOffset(mq MessageQueue) (int64, error)
	removeOffset(mq MessageQueue)
	persist(mq MessageQueue) error
}

func NewOffsetStore(model MessageModel) OffsetStore {
	return nil
}
