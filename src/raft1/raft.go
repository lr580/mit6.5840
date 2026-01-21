package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int //0 init
	votedFor    int //peers下标, -1代表null
	log         []LogEntry
	// 快照会截断日志，因此需要记录被截断部分的最后一个索引和任期
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int

	state     int //Leader/Follower/Candidate
	applyCh   chan raftapi.ApplyMsg
	closeOnce sync.Once

	// Follower 和 Candidate 如果在一个选举 timeout 内没有收到 leader 的心跳或新的投票请求，就会触发新一轮选举；但一旦收到合法的 AppendEntries 或投出一票，就要重置这个计时器
	electionDeadline  time.Time
	heartbeatInterval time.Duration //同理，心跳计时器，Leader 用
	heartbeatDue      time.Time
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := time.Duration(600+rand.Intn(300)) * time.Millisecond
	rf.electionDeadline = time.Now().Add(electionTimeout)
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// return lastIndex, lastTerm
func (rf *Raft) getLastLog() (int, int) {
	return rf.lastLogIndex(), rf.lastLogTerm()
}

func (rf *Raft) logIndexToOffset(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) getLogTerm(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	offset := rf.logIndexToOffset(index)
	if offset <= 0 || offset >= len(rf.log) {
		return -1
	}
	return rf.log[offset].Term
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	offset := rf.logIndexToOffset(index)
	if offset < 0 || offset >= len(rf.log) {
		panic("log entry index out of range")
	}
	return rf.log[offset]
}

// updateTerm 在持锁状态下处理遇到更高任期时的降级逻辑。need lock
func (rf *Raft) updateTerm(term int) {
	if term <= rf.currentTerm {
		return
	}
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
	rf.persist()
	rf.resetElectionTimer()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// **NEED LOCK**
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.log); err != nil {
		panic(err)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	snapshot := rf.persister.ReadSnapshot()
	rf.snapshot = snapshot
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	var log []LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		panic(err)
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&log); err != nil {
		panic(err)
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	if len(log) == 0 {
		// 保险起见保持哨兵元素，防止后续访问 log[0] 越界
		log = []LogEntry{{Term: lastIncludedTerm, Command: nil}}
	}
	rf.log = log
	if rf.commitIndex < rf.lastIncludedIndex { //否则无法索引
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex { // 重复请求
		return
	}
	if index > rf.commitIndex {
		return
	}
	offset := rf.logIndexToOffset(index)
	if offset <= 0 || offset >= len(rf.log) {
		return
	}
	rf.lastIncludedTerm = rf.log[offset].Term
	rf.log = append([]LogEntry{{Term: rf.lastIncludedTerm, Command: nil}}, rf.log[offset+1:]...)
	rf.lastIncludedIndex = index
	for i := range rf.nextIndex {
		if rf.nextIndex[i] < rf.lastIncludedIndex+1 {
			rf.nextIndex[i] = rf.lastIncludedIndex + 1
		}
		if rf.matchIndex[i] < rf.lastIncludedIndex {
			rf.matchIndex[i] = rf.lastIncludedIndex
		}
	}
	if rf.commitIndex < index { //否则无法索引
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.snapshot = cloneBytes(snapshot)
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false   //拒绝
		reply.Term = rf.currentTerm //自己的任期
		return
	}
	if args.Term > rf.currentTerm { //落后
		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm // args.Term > 会更新

	lastIndex, lastTerm := rf.getLastLog()
	candidateUpToDate := (args.LastLogTerm > lastTerm) ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateUpToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// origin codes
	// index := -1
	// term := -1
	// isLeader := true
	// Your code here (3B).
	rf.mu.Lock()
	if rf.state != Leader {
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}
	term := rf.currentTerm
	index := rf.lastLogIndex() + 1
	entry := LogEntry{Term: term, Command: command}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()
	rf.mu.Unlock()

	rf.broadcastAppendEntries()

	return index, term, true
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.heartbeatDue = time.Now().Add(rf.heartbeatInterval)
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateLogToPeer(peer)
	}
}

func (rf *Raft) replicateLogToPeer(server int) {
	// 只发送一次，不要无限循环
	// 下一次心跳会重新调用这个函数
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	firstIndex := rf.lastIncludedIndex + 1
	if rf.nextIndex[server] < firstIndex {
		if len(rf.snapshot) == 0 {
			rf.nextIndex[server] = firstIndex
		} else {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              cloneBytes(rf.snapshot),
			}
			rf.mu.Unlock()
			reply := InstallSnapshotReply{}
			if !rf.sendInstallSnapshot(server, &args, &reply) {
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.updateTerm(reply.Term)
				rf.mu.Unlock()
				return
			}
			if rf.state != Leader || args.Term != rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.mu.Unlock()
			return
		}
	}
	lastIndex := rf.lastLogIndex()
	if rf.nextIndex[server] > lastIndex+1 {
		rf.nextIndex[server] = lastIndex + 1
	}
	nextIndex := rf.nextIndex[server]
	prevIndex := nextIndex - 1
	prevTerm := rf.getLogTerm(prevIndex)
	start := rf.logIndexToOffset(nextIndex)
	entries := make([]LogEntry, len(rf.log)-start)
	copy(entries, rf.log[start:])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if !rf.sendAppendEntries(server, &args, &reply) {
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		rf.mu.Unlock()
		return
	}
	if rf.state != Leader || args.Term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[server] {
			rf.matchIndex[server] = match
			rf.nextIndex[server] = match + 1
		}
		rf.updateLeaderCommit()
		rf.mu.Unlock()
		return
	}
	rf.adjustNextIndexOnFailure(server, reply)
	rf.mu.Unlock()
}

// need lock
func (rf *Raft) adjustNextIndexOnFailure(server int, reply AppendEntriesReply) {
	if reply.ConflictTerm != -1 {
		lastIndex := -1
		for idx := rf.lastLogIndex(); idx > rf.lastIncludedIndex; idx-- {
			if rf.getLogTerm(idx) == reply.ConflictTerm {
				lastIndex = idx
				break
			}
		}
		if lastIndex != -1 {
			rf.nextIndex[server] = lastIndex + 1
			return
		}
	}
	if reply.ConflictIndex > 0 {
		rf.nextIndex[server] = reply.ConflictIndex
		return
	}
	// 上面：优化算法(返回值多了conflicting参数)
	// 下面：朴素算法(返回值只有两个参数)
	minIndex := rf.lastIncludedIndex + 1
	if rf.nextIndex[server] > minIndex {
		rf.nextIndex[server]--
	} else {
		rf.nextIndex[server] = minIndex
	}
}

// need lock
func (rf *Raft) updateLeaderCommit() {
	majority := len(rf.peers)/2 + 1
	for idx := rf.lastLogIndex(); idx > rf.commitIndex; idx-- {
		if rf.getLogTerm(idx) != rf.currentTerm {
			continue
		}
		count := 1 // leader 自己
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= majority {
			rf.commitIndex = idx
			break
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.closeOnce.Do(func() { close(rf.applyCh) })
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) requestVoteFromPeer(server int, args RequestVoteArgs, term int, majority int, votes *int, won *bool) {
	reply := RequestVoteReply{}
	if !rf.sendRequestVote(server, &args, &reply) {
		return
	}

	rf.mu.Lock()
	needHeartbeat := false // 防止死锁，不能 defer
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm != term || rf.state != Candidate || *won {
		rf.mu.Unlock()
		return
	}
	if reply.VoteGranted {
		*votes = *votes + 1
		if *votes >= majority && !*won {
			*won = true
			rf.becomeLeader()
			needHeartbeat = true
		}
	}
	rf.mu.Unlock()
	if needHeartbeat {
		rf.sendHeartbeats()
	}
}

// need lock
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	lastIndex, _ := rf.getLastLog()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
	rf.matchIndex[rf.me] = lastIndex
	rf.heartbeatDue = time.Now()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	term := rf.currentTerm
	lastIndex, lastTerm := rf.getLastLog()
	rf.persist()
	rf.resetElectionTimer()
	votes := 1
	won := false
	majority := len(rf.peers)/2 + 1
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}
		go rf.requestVoteFromPeer(peer, args, term, majority, &votes, &won)
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.broadcastAppendEntries()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = -1
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}
	rf.state = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm // updateTerm 可能会更新

	// 优化算法(返回值多了conflicting参数)
	// 如果朴素算法，两个 if 里只有 reply.Success = false (直接return)
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}

	prevTerm := rf.getLogTerm(args.PrevLogIndex)
	if prevTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevTerm
		conflictIndex := args.PrevLogIndex
		for conflictIndex > rf.lastIncludedIndex && rf.getLogTerm(conflictIndex-1) == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	insertion := args.PrevLogIndex + 1
	i := 0
	for ; i < len(args.Entries); i++ {
		pos := insertion + i
		if pos > rf.lastLogIndex() {
			break
		}
		if rf.getLogTerm(pos) != args.Entries[i].Term {
			offset := rf.logIndexToOffset(pos)
			rf.log = rf.log[:offset]
			break
		}
	}
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
	}
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		last := rf.lastLogIndex()
		if args.LeaderCommit < last {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = last
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}
	rf.state = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	snapshotCopy := cloneBytes(args.Data)

	if args.LastIncludedIndex <= rf.lastLogIndex() {
		offset := rf.logIndexToOffset(args.LastIncludedIndex)
		if offset >= 0 && offset < len(rf.log) && rf.getLogTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
			rf.log = append([]LogEntry{{Term: args.LastIncludedTerm, Command: nil}}, rf.log[offset+1:]...)
		} else {
			rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
		}
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = snapshotCopy
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	rf.persist()

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshotCopy,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.mu.Unlock()

	// 在发送前检查是否已经 killed，避免 panic
	if rf.killed() {
		return
	}
	rf.applyCh <- msg
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			index := rf.lastApplied
			entry := rf.getLogEntry(index)
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: index,
			}
			rf.mu.Unlock()
			// 在发送前检查是否已经 killed，避免 panic
			if rf.killed() {
				return
			}
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock() // RPC 等会改变成员属性
		now := time.Now()
		shouldElect := rf.state != Leader && now.After(rf.electionDeadline)
		shouldHeartbeat := rf.state == Leader && now.After(rf.heartbeatDue)
		rf.mu.Unlock()

		if shouldElect {
			rf.startElection()
		} else if shouldHeartbeat {
			rf.sendHeartbeats()
		}

		// pause for a random amount of time between 50 and 150
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.log = []LogEntry{{Term: rf.lastIncludedTerm, Command: nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.heartbeatDue = time.Now()
	rf.resetElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastIncludedIndex + 1
		rf.matchIndex[i] = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
