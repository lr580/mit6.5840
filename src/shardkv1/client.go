package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt        *tester.Clnt
	sck         *shardctrler.ShardCtrler
	groupClerks map[tester.Tgid]*shardgrp.Clerk // cache clerks for each group
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:        clnt,
		sck:         sck,
		groupClerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	return ck
}

func (ck *Clerk) doOp(key string, value string, version rpc.Tversion, isGet bool) (answer string, ver rpc.Tversion, err rpc.Err) {
	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(shard)
		if !ok || gid == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// clerk 可以复用
		groupClerk, exists := ck.groupClerks[gid]
		if !exists {
			groupClerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.groupClerks[gid] = groupClerk
		}

		if isGet {
			answer, ver, err = groupClerk.Get(key)
		} else {
			err = groupClerk.Put(key, value, version)
		}
		if err == rpc.ErrWrongGroup {
			continue
		}
		return answer, ver, err
	}
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	return ck.doOp(key, "", 0, true)
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	_, _, err := ck.doOp(key, value, version, false)
	return err
}
