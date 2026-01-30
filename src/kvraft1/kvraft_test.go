package kvraft

import (
	"fmt"
	//"log"
	"strconv"
	"testing"
	"time"

	"6.5840/featureflag"
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	NCLNT = 10
)

// Basic test is as follows: one or more clients submitting Puts/Gets
// operations to set of servers for some period of time using
// kvtest.OneClientPut.  After the period is over, test checks that all
// puts/gets values form a linearizable history. If unreliable is set,
// RPCs may fail.  If crash is set, the servers crash after the period
// is over and restart.  If partitions is set, the test repartitions
// the network concurrently with the clients and servers. If
// maxraftstate is a positive number, the size of the state for Raft
// (i.e., log size) shouldn't exceed 8*maxraftstate. If maxraftstate
// is negative, snapshots shouldn't be used.
func (ts *Test) GenericTest() {
	const (
		NITER = 3
		NSEC  = 1
		T     = NSEC * time.Second
		NKEYS = 100
	)
	// const T = 1 * time.Millisecond
	defer ts.Cleanup()

	ch_partitioner := make(chan bool)
	ch_spawn := make(chan struct{})
	ck := ts.MakeClerk()
	res := kvtest.ClntRes{}
	default_key := []string{"k"} // if not running with randomkeys
	if ts.randomkeys {
		default_key = kvtest.MakeKeys(NKEYS)
	}
	for i := 0; i < NITER; i++ {
		// log.Printf("Iteration %v\n", i)

		go func() {
			rs := ts.SpawnClientsAndWait(ts.nclients, T, func(cli int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
				return ts.OneClientPut(cli, ck, default_key, done)
			})
			if !ts.randomkeys {
				reliable := ts.IsReliable() && !ts.crash && !ts.partitions
				ts.CheckPutConcurrent(ck, default_key[0], rs, &res, reliable)
			}
			ch_spawn <- struct{}{}
		}()

		if ts.partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(1 * time.Second)
			go ts.Partitioner(Gid, ch_partitioner)
		}

		<-ch_spawn // wait for clients to be done

		if i == NITER-1 {
			tester.SetAnnotationFinalized()
		}

		ts.CheckPorcupine()

		if ts.partitions {
			ch_partitioner <- true
			//log.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			ts.Group(Gid).ConnectAll()
			tester.AnnotateClearFailure()
			// wait for a while so that we have a new term
			time.Sleep(kvtest.ElectionTimeout)
		}

		if ts.crash {
			// log.Printf("shutdown servers\n")
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).ShutdownServer(i)
			}
			tester.AnnotateShutdownAll()
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(kvtest.ElectionTimeout)
			// log.Printf("restart servers\n")
			// crash and re-start all
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).StartServer(i)
			}
			ts.Group(Gid).ConnectAll()
			tester.AnnotateClearFailure()
		}

		if ts.maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint.
			sz := ts.Config.Group(Gid).LogSize()
			if sz > 8*ts.maxraftstate {
				err := fmt.Sprintf("logs were not trimmed (%v > 8*%v)", sz, ts.maxraftstate)
				tester.AnnotateCheckerFailure(err, err)
				ts.t.Fatalf(err)
			}
		}
		if ts.maxraftstate < 0 {
			// Check that snapshots are not used
			ssz := ts.Group(Gid).SnapshotSize()
			if ssz > 0 {
				err := fmt.Sprintf("snapshot too large (%v), should not be used when maxraftstate = %d", ssz, ts.maxraftstate)
				tester.AnnotateCheckerFailure(err, err)
				ts.t.Fatalf(err)
			}
		}
	}
}

// check that ops are committed fast enough, better than 1 per heartbeat interval
func (ts *Test) GenericTestSpeed() {
	const numOps = 1000

	defer ts.Cleanup()

	ck := ts.MakeClerk()

	// wait until first op completes, so we know a leader is elected
	// and KV servers are ready to process client requests
	ck.Get("x")

	start := time.Now()
	for i := 0; i < numOps; i++ {
		if err := ck.Put("k", strconv.Itoa(i), rpc.Tversion(i)); err != rpc.OK {
			ts.t.Fatalf("Put err %v", err)
		}
	}
	dur := time.Since(start)

	if _, ver, err := ck.Get("k"); err != rpc.OK {
		ts.t.Fatalf("Get err %v", err)
	} else if ver != numOps {
		ts.t.Fatalf("Get too few ops %v", ver)
	}

	// heartbeat interval should be ~ 100 ms; require at least 3 ops per
	const heartbeatInterval = 100 * time.Millisecond
	const opsPerInterval = 3
	const timePerOp = heartbeatInterval / opsPerInterval
	if dur > numOps*timePerOp {
		ts.t.Fatalf("Operations completed too slowly %v/op > %v/op\n", dur/numOps, timePerOp)
	}
}

func TestBasic4B(t *testing.T) {
	ts := MakeTest(t, "4B basic", 1, 5, true, false, false, -1, false)
	tester.AnnotateTest("TestBasic4B", ts.nservers)
	ts.GenericTest()
}

func TestSpeed4B(t *testing.T) {
	ts := MakeTest(t, "4B speed", 1, 3, true, false, false, -1, false)
	tester.AnnotateTest("TestSpeed4B", ts.nservers)
	ts.GenericTestSpeed()
}

func TestConcurrent4B(t *testing.T) {
	ts := MakeTest(t, "4B many clients", 5, 5, true, false, false, -1, false)
	tester.AnnotateTest("TestConcurrent4B", ts.nservers)
	ts.GenericTest()
}

func TestUnreliable4B(t *testing.T) {
	ts := MakeTest(t, "4B many clients", 5, 5, false, false, false, -1, false)
	tester.AnnotateTest("TestUnreliable4B", ts.nservers)
	ts.GenericTest()
}

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
func TestOnePartition4B(t *testing.T) {
	ts := MakeTest(t, "4B progress in majority", 0, 5, false, false, false, -1, false)
	defer ts.Cleanup()

	tester.AnnotateTest("TestOnePartition4B", ts.nservers)

	ck := ts.MakeClerk()

	ver0 := ts.PutAtLeastOnce(ck, "1", "13", rpc.Tversion(0), -1)

	foundl, l := rsm.Leader(ts.Config, Gid)
	if foundl {
		text := fmt.Sprintf("leader found = %v", l)
		tester.AnnotateInfo(text, text)
	} else {
		text := "did not find a leader"
		tester.AnnotateInfo(text, text)
	}
	p1, p2 := ts.Group(Gid).MakePartition(l)
	ts.Group(Gid).Partition(p1, p2)
	tester.AnnotateTwoPartitions(p1, p2)

	ckp1 := ts.MakeClerkTo(p1)  // connect ckp1 to p1
	ckp2a := ts.MakeClerkTo(p2) // connect ckp2a to p2
	ckp2b := ts.MakeClerkTo(p2) // connect ckp2b to p2

	ver1 := ts.PutAtLeastOnce(ckp1, "1", "14", ver0, -1)
	ts.CheckGet(ckp1, "1", "14", ver1)

	ts.End()

	done0 := make(chan rpc.Tversion)
	done1 := make(chan rpc.Tversion)

	ts.Begin("Test: no progress in minority (4B)")
	tester.AnnotateCheckerBegin(fmt.Sprintf("submit Put(1, 15) and Get(1) to %v", p2))
	go func() {
		ver := ts.PutAtLeastOnce(ckp2a, "1", "15", ver1, -1)
		done0 <- ver
	}()
	go func() {
		_, ver, _ := ts.Get(ckp2b, "1", -1) // different clerk in p2
		done1 <- ver
	}()

	select {
	case ver := <-done0:
		err := fmt.Sprintf("Put in minority completed with version = %v", ver)
		tester.AnnotateCheckerFailure(err, err)
		t.Fatalf(err)
	case ver := <-done1:
		err := fmt.Sprintf("Get in minority completed with version = %v", ver)
		tester.AnnotateCheckerFailure(err, err)
		t.Fatalf(err)
	case <-time.After(time.Second):
	}
	tester.AnnotateCheckerSuccess(
		"commands to minority partition not committed after 1 second", "not committed")

	ts.CheckGet(ckp1, "1", "14", ver1)
	ver2 := ts.PutAtLeastOnce(ckp1, "1", "16", ver1, -1)
	ts.CheckGet(ckp1, "1", "16", ver2)

	ts.End()

	ts.Begin("Test: completion after heal (4B)")

	ts.Group(Gid).ConnectAll()
	tester.AnnotateClearFailure()
	ckp2a.(*kvtest.TestClerk).Clnt.ConnectAll()
	ckp2b.(*kvtest.TestClerk).Clnt.ConnectAll()

	time.Sleep(kvtest.ElectionTimeout)

	tester.AnnotateCheckerBegin("status of Put(1, 15)")
	ver15 := rpc.Tversion(0)
	select {
	case ver15 = <-done0:
	case <-time.After(30 * 100 * time.Millisecond):
		tester.AnnotateCheckerFailure(
			"Put(1, 15) did not complete after partition resolved", "OK")
		t.Fatalf("Put did not complete")
	}
	tester.AnnotateCheckerSuccess("Put(1, 15) completed after partition resolved", "OK")

	tester.AnnotateCheckerBegin("status of Get(1)")
	select {
	case <-done1:
	case <-time.After(30 * 100 * time.Millisecond):
		tester.AnnotateCheckerFailure(
			"Get(1) did not complete after partition resolved", "OK")
		t.Fatalf("Get did not complete")
	default:
	}
	tester.AnnotateCheckerSuccess("Get(1) completed after partition resolved", "OK")

	ts.CheckGet(ck, "1", "15", ver15)
}

func TestManyPartitionsOneClient4B(t *testing.T) {
	ts := MakeTest(t, "4B partitions, one client", 1, 5, true, false, true, -1, false)
	tester.AnnotateTest("TestManyPartitionsOneClient4B", ts.nservers)
	ts.GenericTest()
}

func TestManyPartitionsManyClients4B(t *testing.T) {
	ts := MakeTest(t, "4B partitions, many clients (4B)", 5, 5, true, false, true, -1, false)
	tester.AnnotateTest("TestManyPartitionsManyClients4B", ts.nservers)
	ts.GenericTest()
}

func TestPersistOneClient4B(t *testing.T) {
	ts := MakeTest(t, "4B restarts, one client 4B ", 1, 5, true, true, false, -1, false)
	tester.AnnotateTest("TestPersistOneClient4B", ts.nservers)
	ts.GenericTest()
}

func TestPersistConcurrent4B(t *testing.T) {
	ts := MakeTest(t, "4B restarts, many clients", 5, 5, true, true, false, -1, false)
	tester.AnnotateTest("TestPersistConcurrent4B", ts.nservers)
	ts.GenericTest()
}

func TestPersistConcurrentUnreliable4B(t *testing.T) {
	ts := MakeTest(t, "4B restarts, many clients ", 5, 5, false, true, false, -1, false)
	tester.AnnotateTest("TestPersistConcurrentUnreliable4B", ts.nservers)
	ts.GenericTest()
}

func TestPersistPartition4B(t *testing.T) {
	ts := MakeTest(t, "4B restarts, partitions, many clients", 5, 5, true, true, true, -1, false)
	tester.AnnotateTest("TestPersistPartition4B", ts.nservers)
	ts.GenericTest()
}

func TestPersistPartitionUnreliable4B(t *testing.T) {
	ts := MakeTest(t, "4B restarts, partitions, many clients", 5, 5, false, true, true, -1, false)
	tester.AnnotateTest("TestPersistPartitionUnreliable4B", ts.nservers)
	ts.GenericTest()
}

func TestPersistPartitionUnreliableLinearizable4B(t *testing.T) {
	ts := MakeTest(t, "4B restarts, partitions, random keys, many clients", 15, 7, false, true, true, -1, true)
	tester.AnnotateTest("TestPersistPartitionUnreliableLinearizable4B", ts.nservers)
	ts.GenericTest()
}

// if one server falls behind, then rejoins, does it
// recover by using the InstallSnapshot RPC?
// also checks that majority discards committed log entries
// even if minority doesn't respond.
func TestSnapshotRPC4C(t *testing.T) {
	const (
		NSRV         = 3
		MAXRAFTSTATE = 1000
	)
	ts := MakeTest(t, "4C SnapshotsRPC", 0, NSRV, true, false, false, MAXRAFTSTATE, false)
	tester.AnnotateTest("TestSnapshotRPC4C", ts.nservers)
	defer ts.Cleanup()

	ck := ts.MakeClerk()

	ts.Begin("Test: InstallSnapshot RPC (4C)")

	vera := ts.PutAtLeastOnce(ck, "a", "A", rpc.Tversion(0), -1)
	ts.CheckGet(ck, "a", "A", vera)

	verb := rpc.Tversion(0)
	// a bunch of puts into the majority partition.
	ts.Group(Gid).Partition([]int{0, 1}, []int{2})
	tester.AnnotateTwoPartitions([]int{0, 1}, []int{2})
	{
		ck1 := ts.MakeClerkTo([]int{0, 1})
		for i := 0; i < 50; i++ {
			ts.PutAtLeastOnce(ck1, strconv.Itoa(i), strconv.Itoa(i), rpc.Tversion(0), -1)
		}
		time.Sleep(kvtest.ElectionTimeout)
		verb = ts.PutAtLeastOnce(ck1, "b", "B", rpc.Tversion(0), -1)
	}

	// check that the majority partition has thrown away
	// most of its log entries.
	sz := ts.Group(Gid).LogSize()
	if sz > 8*ts.maxraftstate {
		err := fmt.Sprintf("logs were not trimmed (%v > 8*%v)", sz, ts.maxraftstate)
		tester.AnnotateCheckerFailure(err, err)
		t.Fatalf(err)
	}

	// now make group that requires participation of
	// lagging server, so that it has to catch up.
	verc := rpc.Tversion(0)
	ts.Group(Gid).Partition([]int{0, 2}, []int{1})
	tester.AnnotateTwoPartitions([]int{0, 2}, []int{1})
	{
		ck1 := ts.MakeClerkTo([]int{0, 2})
		verc = ts.PutAtLeastOnce(ck1, "c", "C", rpc.Tversion(0), -1)
		ts.PutAtLeastOnce(ck1, "d", "D", rpc.Tversion(0), -1)
		ts.CheckGet(ck1, "a", "A", vera)
		ts.CheckGet(ck1, "b", "B", verb)
		ts.CheckGet(ck1, "1", "1", rpc.Tversion(1))
		ts.CheckGet(ck1, "49", "49", rpc.Tversion(1))
	}

	// now everybody
	ts.Group(Gid).Partition([]int{0, 1, 2}, []int{})
	tester.AnnotateClearFailure()

	vere := ts.PutAtLeastOnce(ck, "e", "E", rpc.Tversion(0), -1)
	ts.CheckGet(ck, "c", "C", verc)
	ts.CheckGet(ck, "e", "E", vere)
	ts.CheckGet(ck, "1", "1", rpc.Tversion(1))
}

// are the snapshots not too huge? 500 bytes is a generous bound for the
// operations we're doing here.
func TestSnapshotSize4C(t *testing.T) {
	ts := MakeTest(t, "4C snapshot size is reasonable", 0, 3, true, false, false, 1000, false)
	tester.AnnotateTest("TestSnapshotSize4C", ts.nservers)
	defer ts.Cleanup()

	maxsnapshotstate := 500

	ck := ts.MakeClerk()

	ver := rpc.Tversion(0)
	for i := 0; i < 200; i++ {
		ver = ts.PutAtLeastOnce(ck, "x", "0", ver, -1)
		ts.CheckGet(ck, "x", "0", ver)
		ver = ts.PutAtLeastOnce(ck, "x", "1", ver, -1)
		ts.CheckGet(ck, "x", "1", ver)
	}

	// check that servers have thrown away most of their log entries
	sz := ts.Group(Gid).LogSize()
	if sz > 8*ts.maxraftstate {
		err := fmt.Sprintf("logs were not trimmed (%v > 8*%v)", sz, ts.maxraftstate)
		tester.AnnotateCheckerFailure(err, err)
		t.Fatalf(err)
	}

	// check that the snapshots are not unreasonably large
	ssz := ts.Group(Gid).SnapshotSize()
	if ssz > maxsnapshotstate {
		err := fmt.Sprintf("snapshot too large (%v > %v)", ssz, maxsnapshotstate)
		tester.AnnotateCheckerFailure(err, err)
		t.Fatalf(err)
	}
}

func TestSpeed4C(t *testing.T) {
	ts := MakeTest(t, "4C speed", 1, 3, true, false, false, 1000, false)
	tester.AnnotateTest("TestSpeed4C", ts.nservers)
	ts.GenericTestSpeed()
}

func TestSnapshotRecover4C(t *testing.T) {
	ts := MakeTest(t, "4C restarts, snapshots, one client", 1, 5, true, true, false, 1000, false)
	tester.AnnotateTest("TestSnapshotRecover4C", ts.nservers)
	ts.GenericTest()
}

func TestSnapshotRecoverManyClients4C(t *testing.T) {
	ts := MakeTest(t, "4C restarts, snapshots, many clients ", 20, 5, true, true, false, 1000, false)
	tester.AnnotateTest("TestSnapshotRecoverManyClients4C", ts.nservers)
	ts.GenericTest()
}

func TestSnapshotUnreliable4C(t *testing.T) {
	ts := MakeTest(t, "4C unreliable net, snapshots, many clients", 5, 5, false, false, false, 1000, false)
	tester.AnnotateTest("TestSnapshotUnreliable4C", ts.nservers)
	ts.GenericTest()
}

func TestSnapshotUnreliableRecover4C(t *testing.T) {
	ts := MakeTest(t, "4C unreliable net, restarts, snapshots, many clients", 5, 5, false, true, false, 1000, false)
	tester.AnnotateTest("TestSnapshotUnreliableRecover4C", ts.nservers)
	ts.GenericTest()
}

func TestSnapshotUnreliableRecoverConcurrentPartition4C(t *testing.T) {
	ts := MakeTest(t, "4C unreliable net, restarts, partitions, snapshots, many clients", 5, 5, false, true, true, 1000, false)
	tester.AnnotateTest("TestSnapshotUnreliableRecoverConcurrentPartition4C", ts.nservers)
	ts.GenericTest()
}

func TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C(t *testing.T) {
	ts := MakeTest(t, "4C unreliable net, restarts, partitions, snapshots, random keys, many clients", 15, 7, false, true, true, 1000, true)
	tester.AnnotateTest("TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C", ts.nservers)
	ts.GenericTest()
}

func TestLeaseFastGetReducesRPCs(t *testing.T) {
	slow := measureLeaseRPCs(t, false)
	fast := measureLeaseRPCs(t, true)
	if fast >= slow {
		t.Fatalf("expected lease fast path to reduce RPCs, slow=%d fast=%d", slow, fast)
	}
}

func TestLeaseWaitsAfterTermSwitch(t *testing.T) {
	restore := setLeaseFlag(true)
	defer restore()

	ts := MakeTest(t, "5D lease term switch", 1, 3, true, false, false, -1, false)
	tester.AnnotateTest("TestLeaseWaitsAfterTermSwitch (5D)", ts.nservers)
	defer ts.Cleanup()

	ck := ts.MakeClerk()
	defer ts.DeleteClerk(ck)

	if err := ck.Put("lease", "init", rpc.Tversion(0)); err != rpc.OK {
		t.Fatalf("initial put failed %v", err)
	}

	leaderIdx, _ := waitForLease(t, ts, 5*time.Second)
	if before := countGetRPCs(ts, ck, "lease"); before > 2 {
		t.Fatalf("expected fast get before switch to be cheap, got %d RPCs", before)
	}

	ts.Group(Gid).ShutdownServer(leaderIdx)
	defer ts.Group(Gid).StartServer(leaderIdx)

	newLeaderIdx := waitForLeaderChange(t, ts, leaderIdx, 5*time.Second)

	if !waitForLeaseState(t, ts, newLeaderIdx, false, time.Second) {
		t.Fatalf("expected new leader to start without lease")
	}

	slowRPCs := countGetRPCs(ts, ck, "lease")
	if slowRPCs < 3 {
		t.Fatalf("expected slow path immediately after term switch, got %d RPCs", slowRPCs)
	}

	waitForSpecificLease(t, ts, newLeaderIdx, 5*time.Second)

	fastRPCs := countGetRPCs(ts, ck, "lease")
	if slowRPCs <= fastRPCs {
		t.Fatalf("expected RPCs to drop once lease re-established, slow=%d fast=%d", slowRPCs, fastRPCs)
	}
}

func TestTxnConditionalPut5D(t *testing.T) {
	restoreTxn := setTxnFlag(true)
	defer restoreTxn()

	ts := MakeTest(t, "5D txn conditional", 1, 3, true, false, false, -1, false)
	tester.AnnotateTest("TestTxnConditionalPut5D", ts.nservers)
	defer ts.Cleanup()

	ckIface := ts.MakeClerk()
	defer ts.DeleteClerk(ckIface)
	ck := unwrapKvraftClerk(ckIface)
	if ck == nil {
		t.Fatalf("failed to unwrap kvraft clerk")
	}

	reply := ck.Txn(
		[]rpc.TxnCompare{{Key: "alpha", Version: 0}},
		[]rpc.TxnOp{{Type: rpc.TxnOpPut, Key: "alpha", Value: "v1"}},
		nil,
	)
	if reply.Err != rpc.OK || !reply.Succeeded {
		t.Fatalf("expected txn success err=%v succeeded=%v", reply.Err, reply.Succeeded)
	}
	if val, _, err := ck.Get("alpha"); err != rpc.OK || val != "v1" {
		t.Fatalf("expected alpha=v1 got (%v,%v)", val, err)
	}

	reply = ck.Txn(
		[]rpc.TxnCompare{{Key: "alpha", Version: 0}},
		[]rpc.TxnOp{{Type: rpc.TxnOpPut, Key: "alpha", Value: "v2"}},
		[]rpc.TxnOp{{Type: rpc.TxnOpPut, Key: "beta", Value: "fallback"}},
	)
	if reply.Err != rpc.OK || reply.Succeeded {
		t.Fatalf("expected txn failure err=%v succeeded=%v", reply.Err, reply.Succeeded)
	}
	if val, _, err := ck.Get("alpha"); err != rpc.OK || val != "v1" {
		t.Fatalf("alpha changed on failed txn, val=%v err=%v", val, err)
	}
	if val, _, err := ck.Get("beta"); err != rpc.OK || val != "fallback" {
		t.Fatalf("failure branch not executed val=%v err=%v", val, err)
	}
}

func TestTxnMultiOps5D(t *testing.T) {
	restoreTxn := setTxnFlag(true)
	defer restoreTxn()

	ts := MakeTest(t, "5D txn multi ops", 1, 3, true, false, false, -1, false)
	tester.AnnotateTest("TestTxnMultiOps5D", ts.nservers)
	defer ts.Cleanup()

	ckIface := ts.MakeClerk()
	defer ts.DeleteClerk(ckIface)
	ck := unwrapKvraftClerk(ckIface)
	if ck == nil {
		t.Fatalf("failed to unwrap kvraft clerk")
	}

	reply := ck.Txn(nil, []rpc.TxnOp{
		{Type: rpc.TxnOpPut, Key: "a", Value: "va1"},
		{Type: rpc.TxnOpPut, Key: "b", Value: "vb1"},
		{Type: rpc.TxnOpGet, Key: "a"},
		{Type: rpc.TxnOpGet, Key: "b"},
	}, nil)
	if reply.Err != rpc.OK || !reply.Succeeded || len(reply.Results) != 4 {
		t.Fatalf("unexpected txn reply %+v", reply)
	}
	if reply.Results[2].Get.Value != "va1" || reply.Results[3].Get.Value != "vb1" {
		t.Fatalf("txn get results incorrect: %+v", reply.Results)
	}

	valA, verA, err := ck.Get("a")
	if err != rpc.OK || valA != "va1" {
		t.Fatalf("Get(a) unexpected (%v,%v)", valA, err)
	}
	valB, verB, err := ck.Get("b")
	if err != rpc.OK || valB != "vb1" {
		t.Fatalf("Get(b) unexpected (%v,%v)", valB, err)
	}

	reply = ck.Txn(
		[]rpc.TxnCompare{
			{Key: "a", Version: verA},
			{Key: "b", Version: verB},
		},
		[]rpc.TxnOp{
			{Type: rpc.TxnOpPut, Key: "a", Value: "va2"},
			{Type: rpc.TxnOpPut, Key: "b", Value: "vb2"},
		},
		nil,
	)
	if reply.Err != rpc.OK || !reply.Succeeded {
		t.Fatalf("expected compare txn success: %+v", reply)
	}

	if valA, _, err = ck.Get("a"); err != rpc.OK || valA != "va2" {
		t.Fatalf("Get(a) after txn unexpected (%v,%v)", valA, err)
	}
	if valB, _, err = ck.Get("b"); err != rpc.OK || valB != "vb2" {
		t.Fatalf("Get(b) after txn unexpected (%v,%v)", valB, err)
	}
}

func measureLeaseRPCs(t *testing.T, enabled bool) int {
	restore := setLeaseFlag(enabled)
	defer restore()

	ts := MakeTest(t, "5D lease rpc compare", 1, 3, true, false, false, -1, false)
	tester.AnnotateTest("TestLeaseRPCCompare (5D)", ts.nservers)
	defer ts.Cleanup()

	ck := ts.MakeClerk()
	defer ts.DeleteClerk(ck)

	if err := ck.Put("lease", "base", rpc.Tversion(0)); err != rpc.OK {
		t.Fatalf("Put failed: %v", err)
	}
	if enabled {
		waitForLease(t, ts, 5*time.Second)
	} else {
		time.Sleep(50 * time.Millisecond)
	}

	const reads = 5
	start := ts.Config.RpcTotal()
	for i := 0; i < reads; i++ {
		if _, _, err := ck.Get("lease"); err != rpc.OK {
			t.Fatalf("Get failed: %v", err)
		}
	}
	return ts.Config.RpcTotal() - start
}

func countGetRPCs(ts *Test, ck kvtest.IKVClerk, key string) int {
	start := ts.Config.RpcTotal()
	if _, _, err := ck.Get(key); err != rpc.OK {
		ts.t.Fatalf("Get failed: %v", err)
	}
	return ts.Config.RpcTotal() - start
}

func setLeaseFlag(enabled bool) func() {
	prev := featureflag.EnableKVFastLeaseGet
	featureflag.EnableKVFastLeaseGet = enabled
	return func() {
		featureflag.EnableKVFastLeaseGet = prev
	}
}

func setTxnFlag(enabled bool) func() {
	prev := featureflag.EnableKVTransactions
	featureflag.EnableKVTransactions = enabled
	return func() {
		featureflag.EnableKVTransactions = prev
	}
}

func unwrapKvraftClerk(ck kvtest.IKVClerk) *Clerk {
	if tck, ok := ck.(*kvtest.TestClerk); ok {
		if inner, ok := tck.IKVClerk.(*Clerk); ok {
			return inner
		}
	}
	if inner, ok := ck.(*Clerk); ok {
		return inner
	}
	return nil
}

func waitForLease(t *testing.T, ts *Test, timeout time.Duration) (int, raftapi.Raft) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if idx, rf, ok := findLeader(ts); ok && rf.HasLease() {
			return idx, rf
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for leader lease")
	return -1, nil
}

func waitForSpecificLease(t *testing.T, ts *Test, index int, timeout time.Duration) {
	if !waitForLeaseState(t, ts, index, true, timeout) {
		t.Fatalf("timeout waiting for server %d lease", index)
	}
}

func waitForLeaseState(t *testing.T, ts *Test, index int, expect bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rf := getRaftAt(ts, index)
		if rf != nil {
			if _, isLeader := rf.GetState(); isLeader {
				if rf.HasLease() == expect {
					return true
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func waitForLeaderChange(t *testing.T, ts *Test, old int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		services := ts.Group(Gid).Services()
		for idx, svcs := range services {
			if idx == old {
				continue
			}
			for _, svc := range svcs {
				if rf, ok := svc.(raftapi.Raft); ok {
					if _, isLeader := rf.GetState(); isLeader {
						return idx
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for new leader (old %d)", old)
	return -1
}

func findLeader(ts *Test) (int, raftapi.Raft, bool) {
	services := ts.Group(Gid).Services()
	for idx, svcs := range services {
		for _, svc := range svcs {
			if rf, ok := svc.(raftapi.Raft); ok {
				if _, isLeader := rf.GetState(); isLeader {
					return idx, rf, true
				}
			}
		}
	}
	return -1, nil, false
}

func getRaftAt(ts *Test, index int) raftapi.Raft {
	services := ts.Group(Gid).Services()
	if index < 0 || index >= len(services) {
		return nil
	}
	for _, svc := range services[index] {
		if rf, ok := svc.(raftapi.Raft); ok {
			return rf
		}
	}
	return nil
}
