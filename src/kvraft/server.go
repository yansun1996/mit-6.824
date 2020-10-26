package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

var kvOnce sync.Once

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type PutAppendCommand struct {
	AppendCh chan (bool)
	Req      PutAppendArgs
}

type GetCommand struct {
	GetCh chan (GetReply)
	Req   GetArgs
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs    map[string]string
	msgIDs map[int64]int64
	killCh chan (bool)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := GetCommand{
		GetCh: make(chan (GetReply)),
		Req:   *args,
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "not leader"
	}
	reply.WrongLeader = false
	select {
	case *reply = <-command.GetCh:
	case <-time.After(time.Millisecond * 700):
		reply.Err = "timeout"
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := PutAppendCommand{
		AppendCh: make(chan (bool)),
		Req:      *args,
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error leader."
	}
	reply.WrongLeader = false
	select {
	case <-command.AppendCh:
		reply.Err = ""
	case <-time.After(time.Millisecond * 700):
		reply.Err = "timeout"
	}
}

func (kv *KVServer) putAppend(args *PutAppendArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("PutAppend: sender: %v msgID: %v op: %v key: %v val: %v\n", args.SenderID, args.MsgID, args.Op, args.Key, args.Value)
	if args.Op == "Put" {
		kv.kvs[args.Key] = args.Value
	} else if args.Op == "Append" {
		value, ok := kv.kvs[args.Key]
		if !ok {
			value = ""
		}
		value += args.Value
		kv.kvs[args.Key] = value
	}
}

func (kv *KVServer) get(args *GetArgs) (reply GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.WrongLeader = false
	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
		reply.Err = ""
	} else {
		reply.Value = ""
		reply.Err = ""
	}
	DPrintf("Get: key: %v err: %v val: %v\n", args.Key, reply.Err, reply.Value)
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killCh <- true
}

// isRepeated check if key exists or not and up to date or not
func (kv *KVServer) isRepeated(args *PutAppendArgs) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	index, ok := kv.msgIDs[args.SenderID]
	if ok {
		if index < args.MsgID {
			// is it is out of date, update the index
			kv.msgIDs[args.SenderID] = args.MsgID
			return false
		}
		return true
	}
	// update msgID after checking duplication
	kv.msgIDs[args.SenderID] = args.MsgID
	return false
}

func (kv *KVServer) onApply(msg raft.ApplyMsg) {
	if command, ok := msg.Command.(PutAppendCommand); ok {
		// when it is put append command
		//DPrintf("cmd: %+v appendCh: %+v req:%+v", command, command.AppendCh, command.Req)
		if !kv.isRepeated(&command.Req) {
			kv.putAppend(&command.Req)
		}
		select {
		case command.AppendCh <- true:
		default:
		}
	} else {
		// whem it is get command
		command := msg.Command.(GetCommand)
		select {
		case command.GetCh <- kv.get(&command.Req):
		default:
		}
	}
}

func (kv *KVServer) mainRoutine() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			// when there is msg coming
			//DPrintf("msg: %v\n", msg.Command)
			kv.onApply(msg)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.kvs = make(map[string]string)
	kv.msgIDs = make(map[int64]int64)
	kv.killCh = make(chan (bool))

	// You may need initialization code here.
	kvOnce.Do(func() {
		labgob.Register(PutAppendCommand{})
		labgob.Register(GetCommand{})
		log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
	})
	go kv.mainRoutine()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
