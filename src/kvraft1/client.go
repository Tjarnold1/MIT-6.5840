package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader int
	mu     sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and Version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	req := rpc.GetArgs{Key: key}
	var lastResp rpc.GetReply
	for {
		resp := rpc.GetReply{}
		ch := make(chan bool, 1)
		go func() {
			ch <- ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &req, &resp)
		}()
		var ok bool
		select {
		case ok = <-ch:
		case <-time.After(time.Millisecond * 300):
			ck.iterateLeader()
			continue
		}
		if !ok || resp.Err == rpc.ErrWrongLeader {
			ck.iterateLeader()
			continue
		}
		lastResp = resp
		if resp.Err == rpc.ErrNoKey || resp.Err == rpc.OK {
			break
		}
	}
	return lastResp.Value, lastResp.Version, lastResp.Err
}

// Put updates key with value only if the Version in the
// request matches the Version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	req := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	var lastResp rpc.PutReply
	retries := 0
	for {
		resp := rpc.PutReply{}
		ch := make(chan bool, 1)
		go func() {
			ch <- ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &req, &resp)
		}()
		var ok bool
		select {
		case ok = <-ch:
		case <-time.After(time.Millisecond * 300):
			retries++
			ck.iterateLeader()
			continue
		}
		if !ok {
			retries++
			ck.iterateLeader()
			continue
		}
		if resp.Err == rpc.ErrWrongLeader {
			ck.iterateLeader()
			continue
		}
		lastResp = resp
		if retries > 0 && resp.Err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}
		break
	}

	return lastResp.Err
}

func (ck *Clerk) iterateLeader() {
	ck.mu.Lock()
	ck.leader = (ck.leader + 1) % len(ck.servers)
	ck.mu.Unlock()
}
