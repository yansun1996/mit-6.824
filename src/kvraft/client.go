package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

const (
	funcCallRetryFreq = 5 * time.Millisecond
	funcCallTimeOut   = 1 * time.Second
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int
	msgID    int64
	me       int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	ck.msgID = 0
	ck.leaderID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	req := GetArgs{Key: key}
	for {
		select {
		case <-time.After(funcCallTimeOut):
			return ""
		case <-time.After(funcCallRetryFreq):
			resp := GetReply{}
			ok := ck.servers[ck.leaderID].Call("KVServer.Get", &req, &resp)
			if ok && !resp.WrongLeader && resp.Err == "" {
				return resp.Value
			}
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	req := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		SenderID: ck.me,
		MsgID:    atomic.AddInt64(&ck.msgID, 1),
	}
	for {
		resp := PutAppendReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &req, &resp)
		if ok && !resp.WrongLeader && resp.Err == "" {
			break
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(funcCallRetryFreq)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
