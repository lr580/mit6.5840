package shardgrp

import (
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
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

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{
		Key:      key,
		Identity: rpc.Identity{ClientId: ck.clientId, RequestId: ck.requestId},
	}
	ck.requestId++
	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := rpc.GetReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup {
				ck.leader = server
				return reply.Value, reply.Version, reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:      key,
		Value:    value,
		Version:  version,
		Identity: rpc.Identity{ClientId: ck.clientId, RequestId: ck.requestId},
	}
	ck.requestId++
	firstRPC := true

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := rpc.PutReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)

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
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
