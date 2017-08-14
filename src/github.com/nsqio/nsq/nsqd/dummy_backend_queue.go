package nsqd
// 实现BackendQueue接口，但是写入的数据都是丢弃的
//对于临时（#ephemeral结尾）Topic/Channel，在创建时会使用dummyBackendQueue初始化backend，
//dummyBackendQueue只是为了统一临时和非临时Topic/Channel而写的，它只是实现了接口，不做任何实质上的操作，
//因此在内存缓冲区满时直接丢弃消息。这也是临时Topic/Channel和非临时的一个比较大的差别。
type dummyBackendQueue struct {
	readChan chan []byte
}

func newDummyBackendQueue() BackendQueue {
	return &dummyBackendQueue{readChan: make(chan []byte)}
}

func (d *dummyBackendQueue) Put([]byte) error {
	return nil
}

func (d *dummyBackendQueue) ReadChan() chan []byte {
	return d.readChan
}

func (d *dummyBackendQueue) Close() error {
	return nil
}

func (d *dummyBackendQueue) Delete() error {
	return nil
}

func (d *dummyBackendQueue) Depth() int64 {
	return int64(0)
}

func (d *dummyBackendQueue) Empty() error {
	return nil
}
