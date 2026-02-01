package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

type FreezeShardArgs struct {
	Shard    shardcfg.Tshid
	Num      shardcfg.Tnum
	Identity rpc.Identity
}

type FreezeShardReply struct {
	State []byte
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard    shardcfg.Tshid
	State    []byte
	Num      shardcfg.Tnum
	Identity rpc.Identity
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard    shardcfg.Tshid
	Num      shardcfg.Tnum
	Identity rpc.Identity
}

type DeleteShardReply struct {
	Err rpc.Err
}

type TxnId struct {
	ClientId  int64
	RequestId int64
}

type TxnPrepareArgs struct {
	Shard    shardcfg.Tshid
	TxnId    TxnId
	Compare  []rpc.TxnCompare
	Success  []rpc.TxnOp
	Failure  []rpc.TxnOp
	Identity rpc.Identity
}

type TxnPrepareReply struct {
	Err       rpc.Err
	Prepared  bool
	CompareOK bool
}

type TxnCommitArgs struct {
	Shard     shardcfg.Tshid
	TxnId     TxnId
	Succeeded bool
	Identity  rpc.Identity
}

type TxnCommitReply struct {
	Err     rpc.Err
	Results []rpc.TxnOpResult
}

type TxnAbortArgs struct {
	Shard    shardcfg.Tshid
	TxnId    TxnId
	Identity rpc.Identity
}

type TxnAbortReply struct {
	Err rpc.Err
}
