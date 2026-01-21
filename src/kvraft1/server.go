package kvraft

import (
	"bytes"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type ValueVer struct {
	Value   string
	Version rpc.Tversion
}

type LastResult struct {
	RequestId int64
	ReplyType byte
	GetReply  rpc.GetReply
	PutReply  rpc.PutReply
}

const (
	replyNone byte = iota
	replyGet
	replyPut
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	// mu sync.Mutex
	table   map[string]ValueVer
	results map[int64]LastResult
}

func (kv *KVServer) doGet(args rpc.GetArgs) rpc.GetReply {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	valueVer, ok := kv.table[args.Key]
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

func (kv *KVServer) doPut(args rpc.PutArgs) rpc.PutReply {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	valueVer, ok := kv.table[args.Key]
	var reply rpc.PutReply
	if !ok {
		if args.Version == rpc.Tversion(0) {
			kv.table[args.Key] = ValueVer{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if args.Version == valueVer.Version {
			kv.table[args.Key] = ValueVer{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	}
	return reply
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	var (
		clientId, requestId int64
		hasIdent            bool
	)
	if v, ok := req.(rpc.Identifiable); ok {
		clientId, requestId = v.IDs()
		hasIdent = true
		if res, exists := kv.results[clientId]; exists && requestId <= res.RequestId {
			switch res.ReplyType {
			case replyGet:
				return res.GetReply
			case replyPut:
				return res.PutReply
			default:
				return nil
			}
		}
	}

	switch args := req.(type) {
	case rpc.GetArgs:
		reply := kv.doGet(args)
		if hasIdent {
			kv.results[clientId] = LastResult{RequestId: requestId, ReplyType: replyGet, GetReply: reply}
		}
		return reply
	case *rpc.GetArgs:
		if args == nil {
			return rpc.GetReply{Err: rpc.ErrWrongLeader}
		}
		reply := kv.doGet(*args)
		if hasIdent {
			kv.results[clientId] = LastResult{RequestId: requestId, ReplyType: replyGet, GetReply: reply}
		}
		return reply
	case rpc.PutArgs:
		reply := kv.doPut(args)
		if hasIdent {
			kv.results[clientId] = LastResult{RequestId: requestId, ReplyType: replyPut, PutReply: reply}
		}
		return reply
	case *rpc.PutArgs:
		if args == nil {
			return rpc.PutReply{Err: rpc.ErrWrongLeader}
		}
		reply := kv.doPut(*args)
		if hasIdent {
			kv.results[clientId] = LastResult{RequestId: requestId, ReplyType: replyPut, PutReply: reply}
		}
		return reply
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.table); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.results); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}

	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var table map[string]ValueVer
	if d.Decode(&table) == nil && table != nil {
		kv.table = table
	} else {
		kv.table = make(map[string]ValueVer)
	}
	var results map[int64]LastResult
	if d.Decode(&results) == nil && results != nil {
		kv.results = results
	} else {
		kv.results = make(map[int64]LastResult)
	}
}

func (kv *KVServer) doSubmit(req any, reply any) {
	err, result := kv.rsm.Submit(req)

	switch out := reply.(type) {
	case *rpc.GetReply:
		if err != rpc.OK {
			out.Err = err
			return
		}
		if v, ok := result.(rpc.GetReply); ok {
			*out = v
		} else {
			out.Err = rpc.ErrWrongLeader //panic maybe
		}
	case *rpc.PutReply:
		if err != rpc.OK {
			out.Err = err
			return
		}
		if v, ok := result.(rpc.PutReply); ok {
			*out = v
		} else {
			out.Err = rpc.ErrWrongLeader //panic maybe
		}
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	kv.doSubmit(*args, reply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	if args == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	kv.doSubmit(*args, reply)
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(LastResult{})

	kv := &KVServer{
		me:      me,
		table:   make(map[string]ValueVer),
		results: make(map[int64]LastResult),
	}

	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		kv.Restore(snapshot)
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
