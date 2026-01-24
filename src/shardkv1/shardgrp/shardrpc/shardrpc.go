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
