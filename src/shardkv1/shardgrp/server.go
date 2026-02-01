package shardgrp

import (
	"bytes"
	"sort"
	"sync/atomic"

	"6.5840/featureflag"
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type ValueVer struct {
	Value   string
	Version rpc.Tversion
}

type LastResult struct {
	RequestId    int64
	ReplyType    byte
	GetReply     rpc.GetReply
	PutReply     rpc.PutReply
	FreezeReply  shardrpc.FreezeShardReply
	InstallReply shardrpc.InstallShardReply
	DeleteReply  shardrpc.DeleteShardReply
	PrepareReply shardrpc.TxnPrepareReply
	CommitReply  shardrpc.TxnCommitReply
	AbortReply   shardrpc.TxnAbortReply
}

const ( //reply type
	replyNone byte = iota
	replyGet
	replyPut
	replyFreeze
	replyInstall
	replyDelete
	replyPrepare
	replyCommit
	replyAbort
)

type ShardState struct {
	Num     shardcfg.Tnum
	Frozen  bool
	Data    map[string]ValueVer
	Results map[int64]LastResult
	Locks   map[string]shardrpc.TxnId
	Pending map[shardrpc.TxnId]*pendingTxn
}

type pendingTxn struct {
	Keys      []string
	Success   []rpc.TxnOp
	Failure   []rpc.TxnOp
	CompareOK bool
	Prepared  bool
}

type shardSnapshot struct {
	Data    map[string]ValueVer
	Results map[int64]LastResult
}

func makeShardState(num shardcfg.Tnum, frozen bool) *ShardState {
	return &ShardState{
		Num:     num,
		Frozen:  frozen,
		Data:    make(map[string]ValueVer),
		Results: make(map[int64]LastResult),
		Locks:   make(map[string]shardrpc.TxnId),
		Pending: make(map[shardrpc.TxnId]*pendingTxn),
	}
}

func (lr LastResult) toResult() any {
	switch lr.ReplyType {
	case replyGet:
		return lr.GetReply
	case replyPut:
		return lr.PutReply
	case replyFreeze:
		return lr.FreezeReply
	case replyInstall:
		return lr.InstallReply
	case replyDelete:
		return lr.DeleteReply
	case replyPrepare:
		return lr.PrepareReply
	case replyCommit:
		return lr.CommitReply
	case replyAbort:
		return lr.AbortReply
	default:
		return nil
	}
}

func (st *ShardState) cloneSnapshot() shardSnapshot {
	data := make(map[string]ValueVer, len(st.Data))
	for k, v := range st.Data {
		data[k] = v
	}
	results := make(map[int64]LastResult, len(st.Results))
	for k, v := range st.Results {
		results[k] = v
	}
	return shardSnapshot{Data: data, Results: results}
}

// dedup 去重；查询是否执行过
func (st *ShardState) dedupReply(clientId, requestId int64) (any, bool) {
	if st == nil || st.Results == nil {
		return nil, false
	}
	res, ok := st.Results[clientId]
	if !ok || requestId > res.RequestId {
		return nil, false
	}
	return res.toResult(), true
}

func (st *ShardState) storeResult(clientId int64, lr LastResult) {
	if st == nil { // 分片记录为空
		return
	}
	if st.Results == nil {
		st.Results = make(map[int64]LastResult)
	}
	st.Results[clientId] = lr
}

func encodeSnapshot(snap shardSnapshot) ([]byte, rpc.Err) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(snap); err != nil {
		return nil, rpc.ErrWrongLeader
	}
	return w.Bytes(), rpc.OK
}

func decodeSnapshot(data []byte) (shardSnapshot, rpc.Err) {
	if len(data) == 0 {
		return shardSnapshot{
			Data:    make(map[string]ValueVer),
			Results: make(map[int64]LastResult),
		}, rpc.OK
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snap shardSnapshot
	if d.Decode(&snap) != nil {
		return shardSnapshot{}, rpc.ErrWrongLeader
	}
	if snap.Data == nil {
		snap.Data = make(map[string]ValueVer)
	}
	if snap.Results == nil {
		snap.Results = make(map[int64]LastResult)
	}
	return snap, rpc.OK
}

type KVServer struct {
	me         int
	dead       int32 // set by Kill()
	rsm        *rsm.RSM
	gid        tester.Tgid
	shards     map[shardcfg.Tshid]*ShardState
	txnEnabled bool
}

func (kv *KVServer) doGet(st *ShardState, args rpc.GetArgs) rpc.GetReply {
	valueVer, ok := st.Data[args.Key]
	var reply rpc.GetReply
	if !ok {
		reply.Err = rpc.ErrNoKey
		return reply
	}
	reply.Err = rpc.OK
	reply.Value = valueVer.Value
	reply.Version = valueVer.Version
	return reply
}

func (kv *KVServer) doPut(st *ShardState, args rpc.PutArgs) rpc.PutReply {
	valueVer, ok := st.Data[args.Key]
	var reply rpc.PutReply
	if !ok {
		if args.Version == rpc.Tversion(0) {
			st.Data[args.Key] = ValueVer{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if args.Version == valueVer.Version {
			st.Data[args.Key] = ValueVer{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	}
	return reply
}

func (st *ShardState) ensureTxn() {
	if st.Locks == nil {
		st.Locks = make(map[string]shardrpc.TxnId)
	}
	if st.Pending == nil {
		st.Pending = make(map[shardrpc.TxnId]*pendingTxn)
	}
}

func uniqueKeys(keys []string) []string {
	set := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if _, ok := set[k]; ok {
			continue
		}
		set[k] = struct{}{}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (st *ShardState) lockKeys(txnId shardrpc.TxnId, keys []string) bool {
	st.ensureTxn()
	locked := make([]string, 0, len(keys))
	for _, key := range keys {
		if owner, ok := st.Locks[key]; ok && owner != txnId {
			for _, lk := range locked {
				delete(st.Locks, lk)
			}
			return false
		}
		st.Locks[key] = txnId
		locked = append(locked, key)
	}
	return true
}

func (st *ShardState) unlockKeys(txnId shardrpc.TxnId, keys []string) {
	if st.Locks == nil {
		return
	}
	for _, key := range keys {
		if owner, ok := st.Locks[key]; ok && owner == txnId {
			delete(st.Locks, key)
		}
	}
}

func (kv *KVServer) doTxnPrepare(st *ShardState, args shardrpc.TxnPrepareArgs) shardrpc.TxnPrepareReply {
	var reply shardrpc.TxnPrepareReply
	if !kv.txnEnabled {
		reply.Err = rpc.ErrTxnDisabled
		return reply
	}
	if st == nil || st.Frozen {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	st.ensureTxn()
	if pending, ok := st.Pending[args.TxnId]; ok && pending.Prepared {
		reply.Err = rpc.OK
		reply.Prepared = true
		reply.CompareOK = pending.CompareOK
		return reply
	}

	keys := make([]string, 0, len(args.Compare)+len(args.Success)+len(args.Failure))
	for _, cmp := range args.Compare {
		keys = append(keys, cmp.Key)
	}
	for _, op := range args.Success {
		keys = append(keys, op.Key)
	}
	for _, op := range args.Failure {
		keys = append(keys, op.Key)
	}
	keys = uniqueKeys(keys)
	if !st.lockKeys(args.TxnId, keys) {
		reply.Err = rpc.ErrTxnConflict
		return reply
	}

	compareOK := true
	for _, cmp := range args.Compare {
		if valueVer, ok := st.Data[cmp.Key]; ok {
			if valueVer.Version != cmp.Version {
				compareOK = false
				break
			}
		} else {
			if cmp.Version != rpc.Tversion(0) {
				compareOK = false
				break
			}
		}
	}

	st.Pending[args.TxnId] = &pendingTxn{
		Keys:      keys,
		Success:   args.Success,
		Failure:   args.Failure,
		CompareOK: compareOK,
		Prepared:  true,
	}
	reply.Err = rpc.OK
	reply.Prepared = true
	reply.CompareOK = compareOK
	return reply
}

func (kv *KVServer) doTxnCommit(st *ShardState, args shardrpc.TxnCommitArgs) shardrpc.TxnCommitReply {
	var reply shardrpc.TxnCommitReply
	if !kv.txnEnabled {
		reply.Err = rpc.ErrTxnDisabled
		return reply
	}
	if st == nil || st.Frozen {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	st.ensureTxn()
	pending, ok := st.Pending[args.TxnId]
	if !ok || !pending.Prepared {
		reply.Err = rpc.ErrTxnConflict
		return reply
	}
	var ops []rpc.TxnOp
	if args.Succeeded {
		ops = pending.Success
	} else {
		ops = pending.Failure
	}
	results := make([]rpc.TxnOpResult, 0, len(ops))
	for _, op := range ops {
		switch op.Type {
		case rpc.TxnOpGet:
			var get rpc.GetReply
			if valueVer, ok := st.Data[op.Key]; ok {
				get.Err = rpc.OK
				get.Value = valueVer.Value
				get.Version = valueVer.Version
			} else {
				get.Err = rpc.ErrNoKey
			}
			results = append(results, rpc.TxnOpResult{Type: rpc.TxnOpGet, Get: get})
		case rpc.TxnOpPut:
			valueVer, ok := st.Data[op.Key]
			var newVersion rpc.Tversion = 1
			if ok {
				newVersion = valueVer.Version + 1
			}
			st.Data[op.Key] = ValueVer{Value: op.Value, Version: newVersion}
			results = append(results, rpc.TxnOpResult{Type: rpc.TxnOpPut, Put: rpc.PutReply{Err: rpc.OK}})
		default:
			results = append(results, rpc.TxnOpResult{Type: op.Type, Put: rpc.PutReply{Err: rpc.ErrWrongLeader}})
		}
	}
	st.unlockKeys(args.TxnId, pending.Keys)
	delete(st.Pending, args.TxnId)
	reply.Err = rpc.OK
	reply.Results = results
	return reply
}

func (kv *KVServer) doTxnAbort(st *ShardState, args shardrpc.TxnAbortArgs) shardrpc.TxnAbortReply {
	var reply shardrpc.TxnAbortReply
	if !kv.txnEnabled {
		reply.Err = rpc.ErrTxnDisabled
		return reply
	}
	if st == nil {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	st.ensureTxn()
	if pending, ok := st.Pending[args.TxnId]; ok {
		st.unlockKeys(args.TxnId, pending.Keys)
		delete(st.Pending, args.TxnId)
	}
	reply.Err = rpc.OK
	return reply
}
func (kv *KVServer) doFreezeShard(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	var reply shardrpc.FreezeShardReply

	state, ok := kv.shards[args.Shard]
	if !ok {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	if args.Num > state.Num {
		state.Frozen = true
		state.Num = args.Num
	}

	snap := state.cloneSnapshot()
	buf, err := encodeSnapshot(snap)
	if err != rpc.OK {
		reply.Err = err
		return reply
	}
	reply.State = buf
	reply.Num = state.Num
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) doInstallShard(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	var reply shardrpc.InstallShardReply

	state, ok := kv.shards[args.Shard]
	if !ok {
		state = makeShardState(0, false)
		kv.shards[args.Shard] = state
	}

	if args.Num <= state.Num {
		reply.Err = rpc.OK
		return reply
	}

	snap, err := decodeSnapshot(args.State)
	if err != rpc.OK {
		reply.Err = err
		return reply
	}

	state.Data = snap.Data
	state.Results = snap.Results
	state.Num = args.Num
	state.Frozen = false
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) doDeleteShard(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	var reply shardrpc.DeleteShardReply

	state, ok := kv.shards[args.Shard]
	if !ok {
		reply.Err = rpc.OK
		return reply
	}

	if args.Num < state.Num {
		reply.Err = rpc.OK
		return reply
	}

	delete(kv.shards, args.Shard)
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) DoOp(req any) any {
	var clientId, requestId int64
	var hasIdent bool
	if v, ok := req.(rpc.Identifiable); ok {
		clientId, requestId = v.IDs()
		hasIdent = true
	}

	var shardId shardcfg.Tshid
	var hasShard bool
	var state *ShardState

	switch args := req.(type) {
	case rpc.GetArgs:
		shardId = shardcfg.Key2Shard(args.Key)
		state, hasShard = kv.shards[shardId]
		if state == nil {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
	case rpc.PutArgs:
		shardId = shardcfg.Key2Shard(args.Key)
		state, hasShard = kv.shards[shardId]
		if state == nil || state.Frozen {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
	case shardrpc.TxnPrepareArgs:
		shardId = args.Shard
		state, hasShard = kv.shards[shardId]
	case shardrpc.TxnCommitArgs:
		shardId = args.Shard
		state, hasShard = kv.shards[shardId]
	case shardrpc.TxnAbortArgs:
		shardId = args.Shard
		state, hasShard = kv.shards[shardId]
	case shardrpc.FreezeShardArgs:
		shardId = args.Shard
		state, hasShard = kv.shards[shardId]
	case shardrpc.InstallShardArgs:
		shardId = args.Shard
		state, hasShard = kv.shards[shardId]
	case shardrpc.DeleteShardArgs:
		shardId = args.Shard
		state, hasShard = kv.shards[shardId]
	}

	if hasIdent && hasShard {
		if res, ok := state.dedupReply(clientId, requestId); ok {
			return res
		}
	}

	switch args := req.(type) {
	case rpc.GetArgs:
		reply := kv.doGet(state, args)
		if hasIdent {
			state.storeResult(clientId, LastResult{RequestId: requestId, ReplyType: replyGet, GetReply: reply})
		}
		return reply
	case rpc.PutArgs:
		reply := kv.doPut(state, args)
		if hasIdent {
			state.storeResult(clientId, LastResult{RequestId: requestId, ReplyType: replyPut, PutReply: reply})
		}
		return reply
	case shardrpc.TxnPrepareArgs:
		reply := kv.doTxnPrepare(state, args)
		if hasIdent && state != nil {
			state.storeResult(clientId, LastResult{RequestId: requestId, ReplyType: replyPrepare, PrepareReply: reply})
		}
		return reply
	case shardrpc.TxnCommitArgs:
		reply := kv.doTxnCommit(state, args)
		if hasIdent && state != nil {
			state.storeResult(clientId, LastResult{RequestId: requestId, ReplyType: replyCommit, CommitReply: reply})
		}
		return reply
	case shardrpc.TxnAbortArgs:
		reply := kv.doTxnAbort(state, args)
		if hasIdent && state != nil {
			state.storeResult(clientId, LastResult{RequestId: requestId, ReplyType: replyAbort, AbortReply: reply})
		}
		return reply
	case shardrpc.FreezeShardArgs:
		reply := kv.doFreezeShard(args)
		if hasIdent {
			if st, ok := kv.shards[args.Shard]; ok {
				st.storeResult(clientId, LastResult{RequestId: requestId, ReplyType: replyFreeze, FreezeReply: reply})
			}
		}
		return reply
	case shardrpc.InstallShardArgs:
		reply := kv.doInstallShard(args)
		if hasIdent {
			if st, ok := kv.shards[args.Shard]; ok {
				st.storeResult(clientId, LastResult{RequestId: requestId, ReplyType: replyInstall, InstallReply: reply})
			}
		}
		return reply
	case shardrpc.DeleteShardArgs:
		reply := kv.doDeleteShard(args)
		if hasIdent {
			if st, ok := kv.shards[args.Shard]; ok {
				st.storeResult(clientId, LastResult{RequestId: requestId, ReplyType: replyDelete, DeleteReply: reply})
			}
		}
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.shards); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		kv.shards = make(map[shardcfg.Tshid]*ShardState)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var shards map[shardcfg.Tshid]*ShardState
	if d.Decode(&shards) == nil && shards != nil {
		for _, st := range shards {
			if st.Data == nil {
				st.Data = make(map[string]ValueVer)
			}
			if st.Results == nil {
				st.Results = make(map[int64]LastResult)
			}
			if st.Locks == nil {
				st.Locks = make(map[string]shardrpc.TxnId)
			}
			if st.Pending == nil {
				st.Pending = make(map[shardrpc.TxnId]*pendingTxn)
			}
		}
		kv.shards = shards
	} else {
		kv.shards = make(map[shardcfg.Tshid]*ShardState)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if v, ok := result.(rpc.GetReply); ok {
		*reply = v
	} else {
		reply.Err = rpc.ErrWrongLeader
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if v, ok := result.(rpc.PutReply); ok {
		*reply = v
	} else {
		reply.Err = rpc.ErrWrongLeader
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if v, ok := result.(shardrpc.FreezeShardReply); ok {
		*reply = v
	} else {
		reply.Err = rpc.ErrWrongLeader
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if v, ok := result.(shardrpc.InstallShardReply); ok {
		*reply = v
	} else {
		reply.Err = rpc.ErrWrongLeader
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if v, ok := result.(shardrpc.DeleteShardReply); ok {
		*reply = v
	} else {
		reply.Err = rpc.ErrWrongLeader
	}
}

func (kv *KVServer) TxnPrepare(args *shardrpc.TxnPrepareArgs, reply *shardrpc.TxnPrepareReply) {
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if v, ok := result.(shardrpc.TxnPrepareReply); ok {
		*reply = v
	} else {
		reply.Err = rpc.ErrWrongLeader
	}
}

func (kv *KVServer) TxnCommit(args *shardrpc.TxnCommitArgs, reply *shardrpc.TxnCommitReply) {
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if v, ok := result.(shardrpc.TxnCommitReply); ok {
		*reply = v
	} else {
		reply.Err = rpc.ErrWrongLeader
	}
}

func (kv *KVServer) TxnAbort(args *shardrpc.TxnAbortArgs, reply *shardrpc.TxnAbortReply) {
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if v, ok := result.(shardrpc.TxnAbortReply); ok {
		*reply = v
	} else {
		reply.Err = rpc.ErrWrongLeader
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(shardrpc.TxnPrepareArgs{})
	labgob.Register(shardrpc.TxnCommitArgs{})
	labgob.Register(shardrpc.TxnAbortArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(LastResult{})
	labgob.Register(ShardState{})
	labgob.Register(shardSnapshot{})
	labgob.Register(rpc.TxnOp{})
	labgob.Register(rpc.TxnOpResult{})
	labgob.Register(rpc.TxnCompare{})
	labgob.Register(shardrpc.TxnId{})
	labgob.Register(shardrpc.TxnPrepareReply{})
	labgob.Register(shardrpc.TxnCommitReply{})
	labgob.Register(shardrpc.TxnAbortReply{})

	kv := &KVServer{
		gid:        gid,
		me:         me,
		shards:     make(map[shardcfg.Tshid]*ShardState),
		txnEnabled: featureflag.EnableShardTransactions,
	}
	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.shards[shardcfg.Tshid(i)] = makeShardState(shardcfg.NumFirst, false)
		}
	}
	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		kv.Restore(snapshot)
	}
	if kv.shards == nil {
		kv.shards = make(map[shardcfg.Tshid]*ShardState)
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
