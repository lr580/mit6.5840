package kvsrv

import (
	"sync"

	"github.com/google/btree"

	"6.5840/featureflag"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type RangeKVServer struct {
	mu      sync.Mutex
	a       map[string]ValueVer
	tree    *btree.BTree
	results map[int64]rangeLastResult
}

type kvItem struct {
	key string
}

func (it kvItem) Less(than btree.Item) bool {
	return it.key < than.(kvItem).key
}

type rangeReplyKind int

const (
	rangeReplyNone rangeReplyKind = iota
	rangeReplyGet
	rangeReplyPut
	rangeReplyRange
)

type rangeLastResult struct {
	requestId int64
	kind      rangeReplyKind
	get       rpc.GetReply
	put       rpc.PutReply
	rng       rpc.RangeReply
}

func MakeRangeKVServer() *RangeKVServer {
	return &RangeKVServer{
		a:       make(map[string]ValueVer),
		tree:    btree.New(16),
		results: make(map[int64]rangeLastResult),
	}
}

func (kv *RangeKVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.handleDuplicateGet(args.Identity, reply) {
		return
	}
	valueVer, ok := kv.a[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Err = rpc.OK
		reply.Value = valueVer.Value
		reply.Version = valueVer.Version
	}
	kv.recordGetResult(args.Identity, reply)
}

func (kv *RangeKVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.handleDuplicatePut(args.Identity, reply) {
		return
	}
	valueVer, ok := kv.a[args.Key]
	if !ok {
		if args.Version == rpc.Tversion(0) {
			kv.a[args.Key] = ValueVer{args.Value, args.Version + 1}
			kv.tree.ReplaceOrInsert(kvItem{key: args.Key})
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if args.Version == valueVer.Version {
			kv.a[args.Key] = ValueVer{args.Value, args.Version + 1}
			kv.tree.ReplaceOrInsert(kvItem{key: args.Key})
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	}

	kv.recordPutResult(args.Identity, reply)
}

func (kv *RangeKVServer) Range(args *rpc.RangeArgs, reply *rpc.RangeReply) {
	if !featureflag.EnableKVRange {
		reply.Err = rpc.ErrNoKey
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.handleDuplicateRange(args.Identity, reply) {
		return
	}

	reply.KVs = reply.KVs[:0]
	kv.tree.AscendGreaterOrEqual(kvItem{key: args.Low}, func(item btree.Item) bool {
		key := item.(kvItem).key
		if args.High != "" && key > args.High {
			return false
		}
		if vv, ok := kv.a[key]; ok {
			reply.KVs = append(reply.KVs, rpc.KeyValue{Key: key, Value: vv.Value, Version: vv.Version})
		}
		return true
	})
	reply.Err = rpc.OK
	kv.recordRangeResult(args.Identity, reply)
}

func (kv *RangeKVServer) handleDuplicateGet(id rpc.Identity, reply *rpc.GetReply) bool {
	if !featureflag.EnableKVExactOnce || id.ClientId == 0 {
		return false
	}
	if res, ok := kv.results[id.ClientId]; ok && res.requestId == id.RequestId && res.kind == rangeReplyGet {
		*reply = res.get
		return true
	}
	return false
}

func (kv *RangeKVServer) handleDuplicatePut(id rpc.Identity, reply *rpc.PutReply) bool {
	if !featureflag.EnableKVExactOnce || id.ClientId == 0 {
		return false
	}
	if res, ok := kv.results[id.ClientId]; ok && res.requestId == id.RequestId && res.kind == rangeReplyPut {
		*reply = res.put
		return true
	}
	return false
}

func (kv *RangeKVServer) handleDuplicateRange(id rpc.Identity, reply *rpc.RangeReply) bool {
	if !featureflag.EnableKVExactOnce || id.ClientId == 0 {
		return false
	}
	if res, ok := kv.results[id.ClientId]; ok && res.requestId == id.RequestId && res.kind == rangeReplyRange {
		*reply = res.rng
		return true
	}
	return false
}

func (kv *RangeKVServer) recordGetResult(id rpc.Identity, reply *rpc.GetReply) {
	if !featureflag.EnableKVExactOnce || id.ClientId == 0 {
		return
	}
	kv.results[id.ClientId] = rangeLastResult{
		requestId: id.RequestId,
		kind:      rangeReplyGet,
		get:       *reply,
	}
}

func (kv *RangeKVServer) recordPutResult(id rpc.Identity, reply *rpc.PutReply) {
	if !featureflag.EnableKVExactOnce || id.ClientId == 0 {
		return
	}
	kv.results[id.ClientId] = rangeLastResult{
		requestId: id.RequestId,
		kind:      rangeReplyPut,
		put:       *reply,
	}
}

func (kv *RangeKVServer) recordRangeResult(id rpc.Identity, reply *rpc.RangeReply) {
	if !featureflag.EnableKVExactOnce || id.ClientId == 0 {
		return
	}
	cp := rpc.RangeReply{Err: reply.Err}
	cp.KVs = append(cp.KVs, reply.KVs...)
	kv.results[id.ClientId] = rangeLastResult{
		requestId: id.RequestId,
		kind:      rangeReplyRange,
		rng:       cp,
	}
}

func (kv *RangeKVServer) Kill() {
}

func StartRangeKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeRangeKVServer()
	return []tester.IService{kv}
}
