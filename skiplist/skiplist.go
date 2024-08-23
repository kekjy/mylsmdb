package skiplist

import (
	"math/rand"
	"mylsmdb/kv"
	"mylsmdb/utils"
	"sync"

	"golang.org/x/exp/constraints"
)

const MaxLevel = 24

type node[K constraints.Ordered, V any] struct {
	key     K
	Value   V
	forward []*node[K, V]
}

func newNode[K constraints.Ordered, V any](key K, value V, level int) *node[K, V] {
	return &node[K, V]{
		key:     key,
		Value:   value,
		forward: make([]*node[K, V], level+1),
	}
}

func newNode2[K constraints.Ordered, V any](level int) *node[K, V] {
	return &node[K, V]{
		forward: make([]*node[K, V], level+1),
	}
}

type SkipList[K constraints.Ordered, V any] struct {
	head  *node[K, V]
	tail  *node[K, V]
	level int
	size  int
	sync.RWMutex
}

func NewSkipList[K constraints.Ordered, V any]() *SkipList[K, V] {
	tail := newNode2[K, V](0)
	head := newNode2[K, V](MaxLevel)
	for i := range head.forward {
		head.forward[i] = tail
	}
	return &SkipList[K, V]{
		head:  head,
		tail:  tail,
		level: 0,
		size:  0,
	}
}

func (sl *SkipList[K, V]) randomLevel() int {
	lv := 1
	for lv < MaxLevel && rand.Intn(2) == 1 {
		lv++
	}
	if lv < MaxLevel {
		return lv
	}
	return MaxLevel
}

func (sl *SkipList[K, V]) Find(key K) *node[K, V] {
	sl.RLock()
	defer sl.RUnlock()
	x := sl.head
	for i := sl.level; i >= 0; i-- {
		for x.forward[i] != sl.tail && x.forward[i].key < key {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x != sl.tail && x.key == key {
		return x
	}
	return sl.tail
}

func (sl *SkipList[K, V]) Get(key K) (V, bool) {
	it := sl.Find(key)
	if it == sl.End() {
		return *new(V), false
	}
	return it.Value, true
}

func (sl *SkipList[K, V]) Put(key K, value V) {
	sl.Lock()
	defer sl.Unlock()
	update := make([]*node[K, V], MaxLevel+1)
	x := sl.head
	for i := sl.level; i >= 0; i-- {
		for x.forward[i] != sl.tail && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if x != sl.tail && x.key == key {
		x.Value = value
		return
	}
	lv := sl.randomLevel()
	if lv > sl.level {
		sl.level++
		lv = sl.level
		update[lv] = sl.head
	}
	newNode := newNode(key, value, lv)
	for i := 0; i <= lv; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	sl.size++
}

func (sl *SkipList[K, V]) Remove(key K) (V, bool) {
	sl.Lock()
	defer sl.Unlock()
	update := make([]*node[K, V], MaxLevel+1)
	x := sl.head
	for i := sl.level; i >= 0; i-- {
		for x.forward[i] != sl.tail && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if x == sl.tail || x.key != key {
		return *new(V), false
	}
	temp_v := x.Value
	for i := 0; i <= x.level(); i++ {
		update[i].forward[i] = x.forward[i]
	}
	for sl.level > 0 && sl.head.forward[sl.level] == sl.tail {
		sl.level--
	}
	sl.size--
	return temp_v, true
}

func (n *node[K, V]) level() int {
	return len(n.forward) - 1
}

func (n *node[K, V]) Next() *node[K, V] {
	return n.forward[0]
}

func (n *node[K, V]) Key() K {
	return n.key
}

func (sl *SkipList[K, V]) Size() int {
	return sl.size
}

func (sl *SkipList[K, V]) Begin() *node[K, V] {
	return sl.head.forward[0]
}

func (sl *SkipList[K, V]) End() *node[K, V] {
	return sl.tail
}

/*func (sl *SkipList[K, V]) GetOrInsert(key K) *V {
	x := sl.Find(key)
	if x != sl.tail {
		return &x.Value
	}
	sl.Put(key, *new(V))
	x = sl.Find(key)
	return &x.Value
}*/

func (sl *SkipList[K, V]) ToValue() []kv.Value {
	sl.RLock()
	defer sl.RUnlock()
	values := make([]kv.Value, 0)
	it := sl.Begin()
	for ; it != sl.End(); it = it.Next() {
		values = append(values, kv.Value{Key: utils.ToString(it.Key()), Value: []byte(utils.ToString(it.Value)), Del: false})
	}
	return values
}
