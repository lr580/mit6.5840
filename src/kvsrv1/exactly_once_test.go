package kvsrv

import (
	"testing"

	"6.5840/featureflag"
	"6.5840/kvsrv1/rpc"
)

func TestKVSrvExactOnceDuplicatePut(t *testing.T) {
	if !featureflag.EnableKVExactOnce {
		t.Skip("exactly-once disabled")
	}

	kv := MakeKVServer()
	args := rpc.PutArgs{
		Key:      "k",
		Value:    "v",
		Version:  0,
		Identity: rpc.Identity{ClientId: 1, RequestId: 1},
	}

	var reply rpc.PutReply
	kv.Put(&args, &reply)
	if reply.Err != rpc.OK {
		t.Fatalf("first put err %v", reply.Err)
	}

	var dup rpc.PutReply
	kv.Put(&args, &dup)
	if dup.Err != rpc.OK {
		t.Fatalf("duplicate put err %v", dup.Err)
	}

	if ver := kv.a["k"].Version; ver != 1 {
		t.Fatalf("expected single version increment, got %d", ver)
	}
}

func TestKVSrvExactOnceDuplicateGet(t *testing.T) {
	if !featureflag.EnableKVExactOnce {
		t.Skip("exactly-once disabled")
	}

	kv := MakeKVServer()
	put := rpc.PutArgs{
		Key:      "k",
		Value:    "v",
		Version:  0,
		Identity: rpc.Identity{ClientId: 2, RequestId: 1},
	}
	var putReply rpc.PutReply
	kv.Put(&put, &putReply)

	args := rpc.GetArgs{
		Key:      "k",
		Identity: rpc.Identity{ClientId: 2, RequestId: 2},
	}

	var reply rpc.GetReply
	kv.Get(&args, &reply)
	if reply.Err != rpc.OK || reply.Value != "v" || reply.Version != 1 {
		t.Fatalf("first get unexpected reply %#v", reply)
	}

	var dup rpc.GetReply
	kv.Get(&args, &dup)
	if dup.Err != rpc.OK || dup.Value != "v" || dup.Version != 1 {
		t.Fatalf("duplicate get unexpected reply %#v", dup)
	}
}
