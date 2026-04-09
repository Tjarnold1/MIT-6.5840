package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"slices"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Status string

const (
	StatusFollower  Status = "follower"
	StatusLeader    Status = "leader"
	StatusCandidate Status = "candidate"
	NoVote          int    = -1
)

const grpcTimeout time.Duration = time.Duration(1000) * time.Millisecond

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

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex   int
	lastApplied   int
	status        Status
	votesAcquired int

	// Volatile leader state
	nextIndex  []int
	matchIndex []int

	// My ideas for how things should work
	electionTimeoutCh chan struct{}
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == StatusLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
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
	var log []LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		panic("error decoding current term")
	}
	if err := d.Decode(&votedFor); err != nil {
		panic("error decoding voted for")
	}
	if err := d.Decode(&log); err != nil {
		panic("error decoding log")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = slices.Clone(log)
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.status = StatusFollower
		rf.votesAcquired = 0
		reply.Term = rf.currentTerm
	}
	upToDateLog := rf.log[len(rf.log)-1].Term < args.LastLogTerm ||
		(args.LastLogIndex >= len(rf.log)-1 && rf.log[len(rf.log)-1].Term == args.LastLogTerm)
	if (rf.votedFor == NoVote ||
		rf.votedFor == args.CandidateId) &&
		upToDateLog {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		rf.votedFor = NoVote
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	BackupIndex int
	BackupTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Check if this leader is for the most up-to-date term
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// Send election timeout
	select {
	case rf.electionTimeoutCh <- struct{}{}:
		// Election timeout sent
	default:
		// Channel was already full
	}
	// Check if this peer needs its term updated
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = StatusFollower
		rf.votedFor = args.LeaderId
		rf.votesAcquired = 0
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
	}
	// Is there an entry at the provided index
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.BackupIndex = len(rf.log) - 1
		return
	}
	// Is the term the same at the index
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		termToSkip := rf.log[args.PrevLogIndex].Term
		var ind int
		for ind = args.PrevLogIndex; ind > 0; ind-- {
			if rf.log[ind].Term != termToSkip {
				reply.BackupIndex = ind
				reply.BackupTerm = rf.log[reply.BackupIndex].Term
				break
			}
		}
		rf.log = rf.log[:ind+1]
		reply.Success = false
		return
	}
	// All checks passed. Append the logs...
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	if args.CommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.CommitIndex, len(rf.log)-1)
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
	index := -1
	term := -1

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != StatusLeader || rf.killed() {
		return index, term, false
	}

	entry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	defer rf.persist()
	rf.matchIndex[rf.me]++
	rf.nextIndex[rf.me]++
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			Entries:      slices.Clone(rf.log[rf.nextIndex[i]:]),
			CommitIndex:  rf.commitIndex,
		}
		go rf.startAppendRequest(i, args)
	}

	return len(rf.log) - 1, rf.currentTerm, rf.status == StatusLeader
}

func (rf *Raft) startAppendRequest(peer int, args *AppendEntriesArgs) {
	resp := &AppendEntriesReply{}
	ok := rf.grpcWithRetry("Raft.AppendEntries", peer, args, resp, 1)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if resp.Success && rf.status == StatusLeader {
			rf.matchIndex[peer] = len(args.Entries) + args.PrevLogIndex
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimeoutCh:
			// Continue on to timeout
		default:
			// Begin election
			rf.mu.Lock()
			if rf.status != StatusLeader {
				go rf.beginElection()
			}
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds. (Larger due to testing requirements)
		ms := 300 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) beginElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.currentTerm++
	rf.status = StatusCandidate
	rf.electionTimeoutCh <- struct{}{} // Reset election timer
	rf.votedFor = rf.me
	rf.votesAcquired = 1
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.requestVote(i, req)
	}
}

func (rf *Raft) grpcWithRetry(method string, server int, args interface{}, reply interface{}, tryCount int) bool {
	c := make(chan bool, 1)
	for i := 0; i < tryCount; i++ {
		go func() {
			c <- rf.peers[server].Call(method, args, reply)
		}()
		select {
		case <-c:
			return true
		case <-time.After(grpcTimeout):
			// Try again
		}
	}
	return false
}

func (rf *Raft) requestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.grpcWithRetry("Raft.RequestVote", server, args, &reply, 1)
	if !ok {
		return
	}
	// Has the state changed in a way that means we don't need to process this
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != StatusCandidate {
		return
	}
	if reply.Term > rf.currentTerm {
		defer rf.persist()
		rf.currentTerm = reply.Term
		rf.status = StatusFollower
		rf.votesAcquired = 0
		rf.votedFor = NoVote
		return
	}
	if reply.VoteGranted {
		rf.votesAcquired++
	}
	if rf.votesAcquired >= len(rf.peers)/2+1 && rf.status == StatusCandidate {
		rf.status = StatusLeader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		go rf.heartbeat()
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status != StatusLeader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				Entries:      slices.Clone(rf.log[rf.nextIndex[i]:]),
				CommitIndex:  rf.commitIndex,
			}
			go func(peer int, args *AppendEntriesArgs) {
				resp := &AppendEntriesReply{}
				ok := rf.peers[peer].Call("Raft.AppendEntries", args, resp)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// Do we still think we're the leader?
				if rf.status != StatusLeader {
					return
				}
				// We do. Is there reason to think we aren't?
				if resp.Term > rf.currentTerm {
					defer rf.persist()
					rf.currentTerm = resp.Term
					rf.status = StatusFollower
					rf.votedFor = NoVote
					rf.votesAcquired = 0
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					return
				}
				// According to this peer, we're still leader
				// Is this peers log aligned with ours?
				if !resp.Success {
					if resp.BackupIndex > 1 {
						rf.nextIndex[peer] = resp.BackupIndex
					} else {
						rf.nextIndex[peer] = 1
					}
					return
				}
				// Update peer information
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			}(i, args)
			go rf.updateCommitIndex()
		}
		rf.mu.Unlock()

		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	greaterThanCommit := 1 // Set at one because we know that we have the log
	for {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				yup
				continue
			}
			if rf.matchIndex[i] > rf.commitIndex {
				greaterThanCommit++
			}
		}
		if greaterThanCommit > len(rf.peers)/2 &&
			rf.status == StatusLeader &&
			rf.commitIndex+1 < len(rf.log) {
			rf.commitIndex++
			greaterThanCommit = 1
		} else {
			break
		}
	}
	return
}

func (rf *Raft) applyWatcher(applyCh chan raftapi.ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex && rf.lastApplied+1 < len(rf.log) {
			applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied+1].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.lastApplied++
		}
		rf.mu.Unlock()

		time.Sleep(300 * time.Millisecond)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = NoVote
	rf.electionTimeoutCh = make(chan struct{}, 1)
	rf.electionTimeoutCh <- struct{}{}

	rf.status = StatusFollower
	rf.votesAcquired = 0
	rf.log = []LogEntry{{Term: 0, Command: ""}}

	// start apply watcher
	go rf.applyWatcher(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
