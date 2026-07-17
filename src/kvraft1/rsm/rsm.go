package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

type Value struct {
	Val     string
	Version rpc.Tversion
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	reqId       int
	pending     map[int]Result
	commitInd   int
	snapshotInd int
	submitCond  *sync.Cond
	isLeader    bool
	dead        int32
}

type Result struct {
	Me   int
	Id   int
	Resp any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[int]Result),
	}
	rsm.submitCond = sync.NewCond(&rsm.mu)
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
	go rsm.reader()
	go rsm.leaderWatcher()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	op := Op{
		Me:  rsm.me,
		Id:  rsm.reqId,
		Req: req,
	}
	rsm.reqId++
	rsm.mu.Unlock()
	ind, _, isLeader := rsm.Raft().Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	rsm.isLeader = isLeader
	for {
		if returnVal, ok := rsm.pending[ind]; ok {
			defer rsm.mu.Unlock()
			if returnVal.Id != op.Id || returnVal.Me != op.Me {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, returnVal.Resp
		}
		if ind < rsm.commitInd || !rsm.isLeader || rsm.killed() {
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
		rsm.submitCond.Wait()
	}
}

func (rsm *RSM) reader() {
	for !rsm.killed() {
		select {
		case applyMsg := <-rsm.applyCh:
			if applyMsg.SnapshotValid {
				rsm.mu.Lock()
				rsm.commitInd = max(rsm.commitInd, applyMsg.SnapshotIndex)
				rsm.sm.Restore(applyMsg.Snapshot)
				//rsm.snapshot(applyMsg.SnapshotIndex)
			} else if applyMsg.CommandValid {
				if applyMsg.CommandIndex <= rsm.commitInd {
					continue
				}
				op := applyMsg.Command.(Op)
				resp := Result{
					Me: op.Me,
					Id: op.Id,
				}
				switch op.Req.(type) {
				case Dec:
					resp.Resp = nil
				case Null:
					resp.Resp = &NullRep{}
				default:
					resp.Resp = rsm.sm.DoOp(op.Req)
				}
				rsm.mu.Lock()
				rsm.commitInd = max(rsm.commitInd, applyMsg.CommandIndex)
				rsm.pending[applyMsg.CommandIndex] = resp
				currentSize := rsm.Raft().PersistBytes()
				if currentSize >= rsm.maxraftstate {
					rsm.snapshot(rsm.commitInd)
				}
			} else {
				panic("Not sure how you've done this")
			}
			// Not sure if I want the snapshot before I unlock...
			rsm.mu.Unlock()
			rsm.submitCond.Broadcast()
		}
	}
}

func (rsm *RSM) leaderWatcher() {
	for !rsm.killed() {
		_, isLeader := rsm.rf.GetState()
		rsm.mu.Lock()
		rsm.isLeader = isLeader
		rsm.mu.Unlock()
		rsm.submitCond.Broadcast()
		time.Sleep(time.Millisecond * 100)
	}
}

func (rsm *RSM) Kill() {
	rsm.mu.Lock()
	atomic.StoreInt32(&rsm.dead, 1)
	rsm.submitCond.Broadcast()
	rsm.mu.Unlock()
}

func (rsm *RSM) killed() bool {
	return atomic.LoadInt32(&rsm.dead) == 1
}

func (rsm *RSM) snapshot(currCommitInd int) {
	serverState := rsm.sm.Snapshot()
	rsm.Raft().Snapshot(currCommitInd, serverState)
}
