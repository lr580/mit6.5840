package kvraft

import (
	"testing"
	"time"

	// "6.5840/featureflag"
	"6.5840/kvsrv1/rpc"
)

func TestKVraftExactOnceDuplicatePut(t *testing.T) {
	// 总是通过，现在就是 exactly-once
	// if !featureflag.EnableKVExactOnce {
	// 	t.Skip("exactly-once disabled")
	// }

	ts := MakeTest(t, "Test (5D): kvraft duplicate put", 1, 3, true, false, false, -1, false)
	defer ts.cleanup()

	clnt := ts.Config.MakeClient()
	defer ts.Config.DeleteClient(clnt)

	args := rpc.PutArgs{
		Key:      "k",
		Value:    "v",
		Version:  0,
		Identity: rpc.Identity{ClientId: 42, RequestId: 1},
	}

	var reply rpc.PutReply
	var target string
	deadline := time.Now().Add(5 * time.Second)
	srvs := ts.Config.Group(Gid).SrvNames()
	for target == "" && time.Now().Before(deadline) {
		for _, srv := range srvs {
			reply = rpc.PutReply{}
			ok := clnt.Call(srv, "KVServer.Put", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				target = srv
				break
			}
		}
		if target == "" {
			time.Sleep(50 * time.Millisecond)
		}
	}

	if target == "" {
		t.Fatalf("no leader accepted request")
	}
	if reply.Err != rpc.OK {
		t.Fatalf("leader put err %v", reply.Err)
	}

	var dup rpc.PutReply
	if ok := clnt.Call(target, "KVServer.Put", &args, &dup); !ok || dup.Err != rpc.OK {
		t.Fatalf("duplicate put err %v ok %v", dup.Err, ok)
	}

	ck := ts.MakeClerk()
	defer ts.DeleteClerk(ck)
	val, ver, err := ck.Get("k")
	if err != rpc.OK || val != "v" || ver != 1 {
		t.Fatalf("unexpected state after duplicate put: val=%q ver=%d err=%v", val, ver, err)
	}
}
