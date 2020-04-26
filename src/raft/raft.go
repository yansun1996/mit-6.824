package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

const (
	Follower  = 1
	Leader    = 2
	Candidate = 3

	HeartbeatDuration   = 150 // Millesecond
	CandidateDuration   = 100 // Millesecond
	MaxHeartbeatTimeout = int64(3)
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	value int
}

type TermLog struct {
	Term int
	logs []Log
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	SenderID  int
	ElectTerm int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrTerm int64
	Granted  bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logs             []TermLog
	heartbeatCounter int64 // num of times trying to receive heartbeat
	roleID           int   // current role
	currTerm         int64 // current term

	heartbeatTimer *time.Timer
	candidateTimer *time.Timer
	randTime       *rand.Rand
	killChan       chan (int)
}

// RequestHeartbeat request heartbeat
type RequestHeartbeat struct {
	SenderID int
	Term     int64
}

// RespHeartbeat heartbeat response
type RespHeartbeat struct {
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = len(rf.logs)
	isleader = rf.roleID == Leader
	return term, isleader
}

func (rf *Raft) getLogValue() int {
	termLogs := rf.logs[len(rf.logs)-1].logs
	logValue := termLogs[len(termLogs)-1].value
	return logValue
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

func (rf *Raft) resetCandidateTimer() {
	randCounter := rf.randTime.Intn(CandidateDuration)
	randDuration := time.Duration(randCounter)*time.Microsecond + time.Duration(MaxHeartbeatTimeout)*HeartbeatDuration*time.Millisecond
	rf.candidateTimer.Reset(time.Duration(randDuration))
	DPrintf("Raft %v candidateTimer reset duration %v\n", rf.me, randDuration)
}

func (rf *Raft) setRole(newRoleID int) {
	if (rf.roleID != Follower) && (newRoleID == Follower) {
		rf.resetCandidateTimer()
	}
	rf.roleID = newRoleID
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// init
	reply.Granted = true
	reply.CurrTerm = rf.currTerm

	// if elect term is older than or equal to then current term, deny
	if args.ElectTerm <= rf.currTerm {
		reply.Granted = false
		return
	}

	// else, turn to follower
	rf.setRole(Follower)
	rf.currTerm = args.ElectTerm
	if reply.Granted {
		rf.heartbeatCounter = 0
	}
}

// HeartbeatFunc function to run when recv heartbeat
func (rf *Raft) HeartbeatFunc(req *RequestHeartbeat, resp *RespHeartbeat) {
	if req.Term > rf.currTerm {
		rf.currTerm = req.Term
		rf.setRole(Follower)
		atomic.StoreInt64(&rf.heartbeatCounter, 0)
	} else if req.Term == rf.currTerm {
		if rf.roleID == Follower {
			atomic.StoreInt64(&rf.heartbeatCounter, 0)
		}
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server int, req *RequestHeartbeat, resp *RespHeartbeat) bool {
	ok := rf.peers[server].Call("Raft.HeartbeatFunc", req, resp)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Vote vote function
func (rf *Raft) Vote() {
	rf.currTerm++

	fmt.Printf("%v starts to vote on term %v \n", rf.me, rf.currTerm)
	req := RequestVoteArgs{
		SenderID:  rf.me,
		ElectTerm: rf.currTerm,
	}
	var wait sync.WaitGroup
	numPeers := len(rf.peers)
	wait.Add(numPeers)
	grantedVote := 0
	term := rf.currTerm
	for i := 0; i < numPeers; i++ {
		go func(idx int) {
			defer wait.Done()
			resp := RequestVoteReply{
				CurrTerm: -1,
				Granted:  false,
			}
			if idx == rf.me {
				grantedVote++
				return
			}
			resultChan := make(chan (bool))
			result := false

			// send request
			go func() {
				result := rf.sendRequestVote(idx, &req, &resp)
				resultChan <- result
			}()

			// wait for result 750ms
			select {
			case result = <-resultChan:
			case <-time.After(750 * time.Microsecond): // rpc timeout
			}

			if !result {
				return
			}

			if resp.Granted {
				grantedVote++
				return
			}

			// update term if it is updated
			if resp.CurrTerm > term {
				term = resp.CurrTerm
			}
		}(i)
	}
	wait.Wait()

	if term > rf.currTerm {
		rf.currTerm = term
		rf.setRole(Follower)
		return
	}

	// the one with most votes become leader
	if grantedVote*2 > numPeers {
		fmt.Printf("New leader: %v \n", rf.me)
		rf.setRole(Leader)

		req := RequestHeartbeat{
			SenderID: rf.me,
			Term:     rf.currTerm,
		}
		for i := 0; i < numPeers; i++ {
			resp := RespHeartbeat{}
			if i != rf.me {
				go func(index int) {
					rf.sendHeartbeat(index, &req, &resp)
				}(i)
			}
		}
	}

}

// heartbeatTimeOutFunc function to run when heartbeat timer timeout
func (rf *Raft) heartbeatTimeOutFunc() {
	if rf.roleID == Follower {
		// follower doesn't receive heartbeat signal
		atomic.AddInt64(&rf.heartbeatCounter, 1)
		DPrintf("Raft %v heartbeatCounter %v\n", rf.me, rf.heartbeatCounter)
	} else if rf.roleID == Leader {
		// leader is responsible for sending heatbeat signal
		for idx := 0; idx < len(rf.peers); idx++ {
			req := RequestHeartbeat{
				SenderID: rf.me,
				Term:     rf.currTerm,
			}
			resp := RespHeartbeat{}
			if idx != rf.me {
				go func(index int) {
					rf.sendHeartbeat(index, &req, &resp)
				}(idx)
			}
		}
		atomic.StoreInt64(&rf.heartbeatCounter, 0)
	} else {
		atomic.StoreInt64(&rf.heartbeatCounter, 0)
	}
	rf.heartbeatTimer.Reset(time.Duration(HeartbeatDuration * time.Millisecond))
	DPrintf("Raft %v heartbeatTimer reset to duration %v\n", rf.me, time.Duration(HeartbeatDuration*time.Millisecond))
}

// candidateTimeOutFunc function to run when candidate timer timeout
func (rf *Raft) candidateTimeOutFunc() {
	if rf.roleID == Follower {
		// if heartbeat timeout surpass max time, turn to candidate and launch voting
		DPrintf("Raft %v heartbeatCounter %v MaxHeartbeatTimeout 3\n", rf.me, rf.heartbeatCounter)
		if rf.heartbeatCounter >= MaxHeartbeatTimeout {
			DPrintf("Raft %v heartbeatCounter surpasses MaxHeartbeatTimeout\n", rf.me)
			rf.setRole(Candidate)
			rf.Vote()
		}
		rf.resetCandidateTimer()
	} else if rf.roleID == Candidate {
		// candidate should launch voting
		rf.Vote()
		rf.resetCandidateTimer()
	}
}

// MainFunc raft main func running in go routine
func (rf *Raft) MainFunc() {
	rf.heartbeatTimer = time.NewTimer(time.Duration(HeartbeatDuration * time.Millisecond))
	rf.candidateTimer = time.NewTimer(time.Duration(0))
	rf.resetCandidateTimer()
	defer func() {
		rf.heartbeatTimer.Stop()
		rf.candidateTimer.Stop()
	}()

	// infinite loop
	for {
		select {
		case <-rf.heartbeatTimer.C:
			DPrintf("Raft %v heartbeatTimer timeout\n", rf.me)
			rf.heartbeatTimeOutFunc()
		case <-rf.candidateTimer.C:
			DPrintf("Raft %v candidateTimer timeout\n", rf.me)
			rf.candidateTimeOutFunc()
		case <-rf.killChan:
			DPrintf("Raft %v killed\n", rf.me)
			return
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.roleID = Follower
	rf.currTerm = 0
	rf.randTime = rand.New((rand.NewSource(time.Now().UnixNano())))
	rf.killChan = make(chan (int))

	go rf.MainFunc()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Printf("Raft %v created\n", rf.me)

	return rf
}
