package shardgrp

import (
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader    int //servers[i], last leader
	clientId  int64
	requestId int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, leader: 0, clientId: rand.Int63(), requestId: 1}
	return ck
}

func (ck *Clerk) callGet(args *rpc.GetArgs) (string, rpc.Tversion, rpc.Err) {
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := rpc.GetReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
					ck.leader = server
					return reply.Value, reply.Version, reply.Err
				}
				if reply.Err == rpc.ErrWrongGroup {
					return reply.Value, reply.Version, reply.Err
				}
			}
		}
		if time.Now().After(deadline) {
			return "", 0, rpc.ErrWrongGroup
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{
		Key:      key,
		Identity: ck.nextIdentity(),
	}
	return ck.callGet(&args)
}

func (ck *Clerk) DoGet(args *rpc.GetArgs) (string, rpc.Tversion, rpc.Err) {
	return ck.callGet(args)
}

func (ck *Clerk) callPut(args *rpc.PutArgs) rpc.Err {
	firstRPC := true
	deadline := time.Now().Add(500 * time.Millisecond)

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := rpc.PutReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", args, &reply)

			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}

			first := firstRPC
			firstRPC = false

			ck.leader = server
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey, rpc.ErrWrongGroup:
				return reply.Err
			case rpc.ErrVersion:
				if first {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
		}
		if time.Now().After(deadline) {
			return rpc.ErrWrongGroup
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:      key,
		Value:    value,
		Version:  version,
		Identity: ck.nextIdentity(),
	}
	return ck.callPut(&args)
}

func (ck *Clerk) DoPut(args *rpc.PutArgs) rpc.Err {
	return ck.callPut(args)
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{
		Shard:    s,
		Num:      num,
		Identity: ck.nextIdentity(),
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := shardrpc.FreezeShardReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.FreezeShard", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			if reply.Err == rpc.OK {
				ck.leader = server
				return reply.State, reply.Err
			}
			if reply.Err == rpc.ErrWrongGroup {
				return nil, reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{
		Shard:    s,
		State:    state,
		Num:      num,
		Identity: ck.nextIdentity(),
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := shardrpc.InstallShardReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.InstallShard", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			if reply.Err == rpc.OK {
				ck.leader = server
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongGroup {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		Shard:    s,
		Num:      num,
		Identity: ck.nextIdentity(),
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := shardrpc.DeleteShardReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.DeleteShard", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			if reply.Err == rpc.OK {
				ck.leader = server
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongGroup {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) nextIdentity() rpc.Identity {
	id := rpc.Identity{ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId++
	return id
}
