package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"math/rand"
	"sort"
	"time"

	"6.5840/featureflag"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type groupEntry struct {
	servers []string
	clerk   *shardgrp.Clerk
}

type Clerk struct {
	clnt        *tester.Clnt
	sck         *shardctrler.ShardCtrler
	groupClerks map[tester.Tgid]*groupEntry // cache clerks for each group
	clientId    int64
	requestId   int64
	txnId       int64
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:        clnt,
		sck:         sck,
		groupClerks: make(map[tester.Tgid]*groupEntry),
		clientId:    rand.Int63(),
		txnId:       1,
	}
	return ck
}

func (ck *Clerk) nextIdentity() rpc.Identity {
	id := rpc.Identity{ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId++
	return id
}

func (ck *Clerk) nextTxnId() shardrpc.TxnId {
	id := shardrpc.TxnId{ClientId: ck.clientId, RequestId: ck.txnId}
	ck.txnId++
	return id
}

func (ck *Clerk) doOp(key string, value string, version rpc.Tversion, isGet bool) (answer string, ver rpc.Tversion, err rpc.Err) {
	identity := ck.nextIdentity()
	var getArgs rpc.GetArgs
	var putArgs rpc.PutArgs
	if isGet {
		getArgs = rpc.GetArgs{Key: key, Identity: identity}
	} else {
		putArgs = rpc.PutArgs{Key: key, Value: value, Version: version, Identity: identity}
	}
	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(shard)
		if !ok || gid == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		groupClerk := ck.ensureGroupClerk(gid, servers)

		if isGet {
			answer, ver, err = groupClerk.DoGet(&getArgs)
		} else {
			err = groupClerk.DoPut(&putArgs)
		}
		if err == rpc.ErrWrongGroup {
			time.Sleep(100 * time.Millisecond)
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

func (ck *Clerk) Txn(compare []rpc.TxnCompare, success []rpc.TxnOp, failure []rpc.TxnOp) rpc.TxnReply {
	if !featureflag.EnableShardTransactions {
		return rpc.TxnReply{Err: rpc.ErrTxnDisabled}
	}
	txnId := ck.nextTxnId()

	for {
		cfg := ck.sck.Query()
		shardData := make(map[shardcfg.Tshid]*txnShardData)
		keysByShard := func(key string) *txnShardData {
			shard := shardcfg.Key2Shard(key)
			entry, ok := shardData[shard]
			if !ok {
				entry = &txnShardData{}
				shardData[shard] = entry
			}
			return entry
		}
		for _, cmp := range compare {
			entry := keysByShard(cmp.Key)
			entry.compare = append(entry.compare, cmp)
		}
		for _, op := range success {
			entry := keysByShard(op.Key)
			entry.success = append(entry.success, op)
		}
		for _, op := range failure {
			entry := keysByShard(op.Key)
			entry.failure = append(entry.failure, op)
		}

		shards := make([]int, 0, len(shardData))
		for shard := range shardData {
			shards = append(shards, int(shard))
		}
		sort.Ints(shards)

		prepared := make([]shardcfg.Tshid, 0, len(shards))
		compareOK := true
		retry := false
		for _, shardInt := range shards {
			shard := shardcfg.Tshid(shardInt)
			gid, servers, ok := cfg.GidServers(shard)
			if !ok || gid == 0 {
				retry = true
				break
			}
			groupClerk := ck.ensureGroupClerk(gid, servers)
			data := shardData[shard]
			reply := groupClerk.TxnPrepare(&shardrpc.TxnPrepareArgs{
				Shard:   shard,
				TxnId:   txnId,
				Compare: data.compare,
				Success: data.success,
				Failure: data.failure,
			})
			if reply.Err == rpc.ErrWrongGroup {
				retry = true
				break
			}
			if reply.Err == rpc.ErrTxnConflict || reply.Err == rpc.ErrTxnDisabled {
				retry = true
				break
			}
			if reply.Err != rpc.OK || !reply.Prepared {
				retry = true
				break
			}
			prepared = append(prepared, shard)
			if !reply.CompareOK {
				compareOK = false
			}
		}

		if retry {
			ck.abortPrepared(cfg, prepared, txnId)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		results := make([]rpc.TxnOpResult, 0)
		for _, shardInt := range shards {
			shard := shardcfg.Tshid(shardInt)
			gid, servers, ok := cfg.GidServers(shard)
			if !ok || gid == 0 {
				return rpc.TxnReply{Err: rpc.ErrWrongGroup}
			}
			groupClerk := ck.ensureGroupClerk(gid, servers)
			reply := groupClerk.TxnCommit(&shardrpc.TxnCommitArgs{
				Shard:     shard,
				TxnId:     txnId,
				Succeeded: compareOK,
			})
			if reply.Err != rpc.OK {
				return rpc.TxnReply{Err: reply.Err}
			}
			results = append(results, reply.Results...)
		}
		return rpc.TxnReply{Err: rpc.OK, Succeeded: compareOK, Results: results}
	}
}

type txnShardData struct {
	compare []rpc.TxnCompare
	success []rpc.TxnOp
	failure []rpc.TxnOp
}

func (ck *Clerk) abortPrepared(cfg *shardcfg.ShardConfig, prepared []shardcfg.Tshid, txnId shardrpc.TxnId) {
	for _, shard := range prepared {
		gid, servers, ok := cfg.GidServers(shard)
		if !ok || gid == 0 {
			continue
		}
		groupClerk := ck.ensureGroupClerk(gid, servers)
		_ = groupClerk.TxnAbort(&shardrpc.TxnAbortArgs{
			Shard: shard,
			TxnId: txnId,
		})
	}
}

func copyServers(servers []string) []string {
	cp := make([]string, len(servers))
	copy(cp, servers)
	return cp
}

func sameServers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (ck *Clerk) ensureGroupClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	if entry, ok := ck.groupClerks[gid]; ok {
		if sameServers(entry.servers, servers) {
			return entry.clerk
		}
	}

	newEntry := &groupEntry{
		servers: copyServers(servers),
		clerk:   shardgrp.MakeClerk(ck.clnt, servers),
	}
	ck.groupClerks[gid] = newEntry
	return newEntry.clerk
}
