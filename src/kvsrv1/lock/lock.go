package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const IDEN_LEN = 8 // 标识符长度，不为它的任意 value 表示空锁
const EMPTY_IDENTIDIER = "0"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	identifier string
	l          string // lock name
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	identifier := kvtest.RandValue(IDEN_LEN)
	lk := &Lock{ck: ck, identifier: identifier, l: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	lk.LoopSet(lk.identifier)
}

func (lk *Lock) Release() {
	// Your code here
	lk.LoopSet(EMPTY_IDENTIDIER)
}

func (lk *Lock) LoopSet(value string) {
	for {
		identifier, version, _ := lk.ck.Get(lk.l)
		// if err == rpc.ErrNoKey { // 本来默认也是 0
		// version = 0 // 感觉不会进入，除非先放再取
		// } // else if err != rpc.OK { // 错误立即重试
		//	continue
		// } 不可能分支 else if
		if len(identifier) == IDEN_LEN && identifier != lk.identifier { // 不是空，不是自己，那么就忙等
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err := lk.ck.Put(lk.l, value, version)
		if err == rpc.OK { // 获取成功
			break
		}
	}
}
