package nsqd

// 基于小堆实现的的优先消息队列
// 父节点一点不会比子节点大
// 不保证左右子节点的大小顺序
// 如果总容量大于25，空闲容量超过一半，则缩容到1/2
// 如果容量不足以两倍的方式扩容
// 缩容扩容都会新建并拷贝整个列表

type inFlightPqueue []*Message

// 创建容量为capacity的切片
func newInFlightPqueue(capacity int) inFlightPqueue {
	return make(inFlightPqueue, 0, capacity)
}

func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *inFlightPqueue) Push(x *Message) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		// 以两倍的方式扩容
		npq := make(inFlightPqueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	x.index = n
	(*pq)[n] = x
	pq.up(n)
}

func (pq *inFlightPqueue) Pop() *Message {
	n := len(*pq)
	c := cap(*pq)
	pq.Swap(0, n-1) // 把最底部的消息跟将要弹出堆的顶部的交换
	pq.down(0, n-1) // 最底部的元素为最小值，马上要弹出，不参与下沉
	if n < (c/2) && c > 25 {
		// 如果总容量大于25，空闲容量超过一半，则缩容到1/2
		npq := make(inFlightPqueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

func (pq *inFlightPqueue) Remove(i int) *Message {
	n := len(*pq)
	if n-1 != i {
		pq.Swap(i, n-1)
		pq.down(i, n-1)
		pq.up(i)
	}
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

// 最小元素的优先级大于max才返回Item
func (pq *inFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*pq) == 0 {
		return nil, 0
	}

	x := (*pq)[0]
	if x.pri > max {
		return nil, x.pri - max
	}
	pq.Pop()

	return x, 0
}

// 上浮
func (pq *inFlightPqueue) up(j int) {
	for {
		i := (j - 1) / 2                            // 计算2叉堆的父节点元素下标
		if i == j || (*pq)[j].pri >= (*pq)[i].pri { // 小堆实现
			break
		}
		pq.Swap(i, j)
		j = i
	}
}

// 下沉
func (pq *inFlightPqueue) down(i, n int) {
	for {
		j1 := 2*i + 1          //计算二叉堆左叶子节点的下标
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		// 跟子节点的小值对比
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri {
			j = j2 // = 2*i + 2  // right child
		}
		// 直到子节点不小于父节点
		if (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		i = j
	}
}
