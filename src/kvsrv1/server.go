package kvsrv

import (
	"log"
	"sync"

	"6.5840/featureflag"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVer struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu      sync.Mutex
	a       map[string]ValueVer
	results map[int64]lastResult
}

type replyKind int

const (
	replyNone replyKind = iota
	replyGet
	replyPut
)

type lastResult struct {
	requestId int64
	kind      replyKind
	get       rpc.GetReply
	put       rpc.PutReply
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		a:       make(map[string]ValueVer),
		results: make(map[int64]lastResult),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
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

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.handleDuplicatePut(args.Identity, reply) {
		return
	}
	valueVer, ok := kv.a[args.Key]
	if !ok {
		if args.Version == rpc.Tversion(0) {
			kv.a[args.Key] = ValueVer{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if args.Version == valueVer.Version {
			kv.a[args.Key] = ValueVer{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	}

	kv.recordPutResult(args.Identity, reply)
}

func (kv *KVServer) exactOnceEnabled(id rpc.Identity) bool {
	return featureflag.EnableKVExactOnce && id.ClientId != 0
}

func (kv *KVServer) handleDuplicateGet(id rpc.Identity, reply *rpc.GetReply) bool {
	if !kv.exactOnceEnabled(id) {
		return false
	}
	if res, ok := kv.results[id.ClientId]; ok && res.requestId == id.RequestId && res.kind == replyGet {
		*reply = res.get
		return true
	}
	return false
}

func (kv *KVServer) handleDuplicatePut(id rpc.Identity, reply *rpc.PutReply) bool {
	if !kv.exactOnceEnabled(id) {
		return false
	}
	if res, ok := kv.results[id.ClientId]; ok && res.requestId == id.RequestId && res.kind == replyPut {
		*reply = res.put
		return true
	}
	return false
}

func (kv *KVServer) recordGetResult(id rpc.Identity, reply *rpc.GetReply) {
	if !kv.exactOnceEnabled(id) {
		return
	}
	kv.results[id.ClientId] = lastResult{
		requestId: id.RequestId,
		kind:      replyGet,
		get:       *reply,
	}
}

func (kv *KVServer) recordPutResult(id rpc.Identity, reply *rpc.PutReply) {
	if !kv.exactOnceEnabled(id) {
		return
	}
	kv.results[id.ClientId] = lastResult{
		requestId: id.RequestId,
		kind:      replyPut,
		put:       *reply,
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
