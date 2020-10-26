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
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const (
	// Follower follower role ID
	Follower = 1
	// Leader follower role ID
	Leader = 2
	// Candidate follower role ID
	Candidate = 3

	// HeartbeatDuration duration for checking heartbeat
	HeartbeatDuration = 1000 * time.Millisecond // Millesecond
	// ElectionDuration duration for checking candidate status
	ElectionDuration = 2000 * time.Millisecond // Millesecond
	// AppendEntriesRPCTimeout timeout for waiting append entries response
	AppendEntriesRPCTimeout = 400 * time.Millisecond
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

// Log info for log entry
type Log struct {
	Term  int
	Index int
	Log   interface{}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	SenderID  int
	ElectTerm int
	LogIndex  int
	LogTerm   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrTerm int
	Granted  bool
}

// AppendEntriesRequest request to append log entries
type AppendEntriesRequest struct {
	Me           int
	Term         int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []Log
	LeaderCommit int
}

// AppendEntriesResp response of appending log entries request
type AppendEntriesResp struct {
	Term        int
	Granted     bool
	LastApplied int
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
	logs                []Log
	commitIndex         int  // commit index
	LastestAppliedIndex int  // last applied index
	roleID              int  // current role
	currTerm            int  // current term
	killed              bool // node is killed or not

	nextIndex []int // next log index of followers
	maxIndex  []int // max log index of followers

	heartbeatTimers []*time.Timer
	electionTimer   *time.Timer
	randTime        *rand.Rand

	applyCh    chan ApplyMsg // apply append log request
	latestLogs AppendEntriesRequest
	enableLogs bool
}

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock()
	defer rf.unlock()
	isleader := rf.roleID == Leader
	return rf.currTerm, isleader
}

func (rf *Raft) setTerm(newTerm int) {
	rf.lock()
	defer rf.unlock()
	rf.currTerm = newTerm
}

func (rf *Raft) addTerm(addedTerm int) {
	rf.lock()
	defer rf.unlock()
	rf.currTerm += addedTerm
}

func (rf *Raft) setCommitIndex(newIndex int) {
	rf.lock()
	defer rf.unlock()
	rf.commitIndex = newIndex
}

func (rf *Raft) getCommitIndex() int {
	rf.lock()
	defer rf.unlock()
	return rf.commitIndex
}

func (rf *Raft) getRole() int {
	rf.lock()
	defer rf.unlock()
	return rf.roleID
}

func (rf *Raft) setRole(newRoleID int) {
	rf.lock()
	defer rf.unlock()
	if (rf.roleID != Follower) && (newRoleID == Follower) {
		rf.resetElectionTimer()
	}

	// log update when node becomes leader
	if rf.roleID != Leader && newRoleID == Leader {
		newIndex := len(rf.logs)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = newIndex + 1
			rf.maxIndex[i] = 0
		}
	}
	rf.roleID = newRoleID
}

func (rf *Raft) setLastestLog(req *AppendEntriesRequest) {
	rf.lock()
	defer rf.unlock()
	rf.latestLogs = *req
}

func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	rf.lock()
	defer rf.unlock()
	idx := 0
	term := 0
	numLogs := len(rf.logs)
	if numLogs > 0 {
		idx = rf.logs[numLogs-1].Index
		term = rf.logs[numLogs-1].Term
	}
	return term, idx
}

func (rf *Raft) getLogTermAtIndex(index int) int {
	rf.lock()
	defer rf.unlock()
	return rf.logs[index-1].Term
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
	rf.lock()
	defer rf.unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.commitIndex)
	e.Encode(rf.LastestAppliedIndex)
	e.Encode(rf.logs)
	e.Encode(rf.latestLogs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.lock()
	defer rf.unlock()
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm, commitIndex, LastestAppliedIndex int
	var logs []Log
	var latestlogs AppendEntriesRequest
	if d.Decode(&currTerm) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&LastestAppliedIndex) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&latestlogs) != nil {
		fmt.Println("Error decoding raft state")
	} else {
		rf.currTerm = currTerm
		rf.commitIndex = commitIndex
		rf.logs = logs
		rf.LastestAppliedIndex = 0
		rf.latestLogs = latestlogs
	}
}

func (rf *Raft) resetElectionTimer() {
	randCounter := rf.randTime.Intn(600)
	randDuration := ElectionDuration + time.Duration(randCounter)*time.Millisecond
	rf.electionTimer.Reset(randDuration)
	//DPrintf("Raft %v electionTimer reset duration %v\n", rf.me, randDuration)
}

func (rf *Raft) resetHeartbeatTimers() {
	rf.lock()
	defer rf.unlock()
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i].Reset(0)
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
	resultChan := make(chan (bool))
	ok := false
	go func() {
		respOk := rf.peers[server].Call("Raft.RequestVote", args, reply)
		resultChan <- respOk
	}()
	select {
	case ok = <-resultChan:
	case <-time.After(HeartbeatDuration):
		//DPrintf("Raft %v wait for vote response from %v rpc timeout", rf.me, server)
	}
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// init
	reply.Granted = true
	reply.CurrTerm, _ = rf.GetState()

	if rf.currTerm >= req.ElectTerm {
		// too old request
		//DPrintf("Raft %v receive too old vote request from raft %v", rf.me, req.SenderID)
		reply.Granted = false
		return
	}
	rf.setRole(Follower)
	rf.setTerm(req.ElectTerm)
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	if lastLogTerm > req.LogTerm {
		// too old request
		//DPrintf("Raft %v receive too old log term in vote request from raft %v", rf.me, req.SenderID)
		reply.Granted = false
	} else if lastLogTerm == req.LogTerm {
		reply.Granted = lastLogIndex <= req.LogIndex
		if !reply.Granted {
			//DPrintf("Raft %v receive too old log index in vote request from raft %v", rf.me, req.SenderID)
		}
	}

	if reply.Granted {
		rf.resetElectionTimer()
	}

	//DPrintf("Raft %v vote granted status is %v for raft %v", rf.me, reply.Granted, req.SenderID)
}

func (rf *Raft) isTooOldLogReq(req *AppendEntriesRequest) bool {
	rf.lock()
	defer rf.unlock()
	if req.Term == rf.latestLogs.Term && req.Me == rf.latestLogs.Me {
		lastIndex := rf.latestLogs.PrevLogIndex + len(rf.latestLogs.Entries)
		reqLastIndex := req.PrevLogIndex + len(req.Entries)
		return lastIndex > reqLastIndex
	}
	return false
}

// sendRequestAppendEntries request to append log entries
func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResp) bool {
	resultChan := make(chan (bool))
	ok := false
	go func() {
		respOk := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
		resultChan <- respOk
	}()
	select {
	case ok = <-resultChan:
	case <-time.After(AppendEntriesRPCTimeout):
		//DPrintf("Raft %v wait for append entries response from %v rpc timeout", rf.me, server)
	}
	return ok
}

// RequestAppendEntries request append log entries function handler
func (rf *Raft) RequestAppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResp) {
	//DPrintf("Raft %v processing append log request", rf.me)
	resp.Granted = true
	resp.Term, _ = rf.GetState()
	if req.Term < resp.Term {
		// old request
		//DPrintf("old append log request")
		resp.Granted = false
		return
	}
	if rf.isTooOldLogReq(req) {
		//DPrintf("too old append log request")
		return
	}

	rf.resetElectionTimer()
	rf.setTerm(req.Term)
	rf.setRole(Follower)

	_, logIndex := rf.getLastLogTermAndIndex()

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > logIndex {
			// req too new
			resp.LastApplied = rf.LastestAppliedIndex
			//DPrintf("too new append log request")
			resp.Granted = false
			return
		}
		if rf.getLogTermAtIndex(req.PrevLogIndex) != req.PrevLogTerm {
			// log term at this index conflict
			resp.LastApplied = rf.LastestAppliedIndex
			//DPrintf("append log request conflict term")
			resp.Granted = false
			return
		}
	}

	rf.setLastestLog(req)
	numReqLogs := len(req.Entries)
	if numReqLogs > 0 {
		rf.updateLog(req.PrevLogIndex, req.Entries)
	}
	rf.setCommitIndex(req.LeaderCommit)
	rf.applyLogs()
	rf.persist()

	return
}

func (rf *Raft) updateLog(index int, logEntries []Log) {
	rf.lock()
	defer rf.unlock()

	for i := 0; i < len(logEntries); i++ {
		if index+i < len(rf.logs) {
			rf.logs[index+i] = logEntries[i]
		} else {
			rf.logs = append(rf.logs, logEntries[i])
		}
	}

	// resize logs in the node
	//DPrintf("Raft: %v index: %v", rf.me, index)
	//DPrintf("Raft: %v len(logEntries): %v", rf.me, len(logEntries))
	//DPrintf("Raft: %v rf.logs: %v", rf.me, rf.logs)
	rf.logs = rf.logs[:index+len(logEntries)]
}

func (rf *Raft) insertLog(command interface{}) int {
	rf.lock()
	defer rf.unlock()
	newLog := Log{
		Term:  rf.currTerm,
		Index: 1,
		Log:   command,
	}

	if len(rf.logs) > 0 {
		newLog.Index = rf.logs[len(rf.logs)-1].Index + 1
	}
	rf.logs = append(rf.logs, newLog)
	return newLog.Index
}

func (rf *Raft) updateCommitIndex() bool {
	rst := false
	var indexs []int
	rf.maxIndex[rf.me] = 0
	if len(rf.logs) > 0 {
		rf.maxIndex[rf.me] = rf.logs[len(rf.logs)-1].Index
	}
	for i := 0; i < len(rf.maxIndex); i++ {
		indexs = append(indexs, rf.maxIndex[i])
	}
	sort.Ints(indexs)
	index := len(indexs) / 2
	commit := indexs[index]
	if commit > rf.commitIndex {
		//DPrintf("Raft %v update leader commit index %v", rf.me, commit)
		rst = true
		rf.commitIndex = commit
	}
	return rst
}

func (rf *Raft) applyLogs() {
	rf.lock()
	defer rf.unlock()
	if rf.roleID == Leader {
		rf.updateCommitIndex()
	}
	latestIndex := 0
	if len(rf.logs) > 0 {
		latestIndex = rf.logs[len(rf.logs)-1].Index
	}
	LastestAppliedIndex := rf.LastestAppliedIndex
	for ; rf.LastestAppliedIndex < rf.commitIndex && rf.LastestAppliedIndex < latestIndex; rf.LastestAppliedIndex++ {
		// it's time to apply the logs that are committed but not applied
		idx := rf.LastestAppliedIndex
		DPrintf("commitIdx: %v latestIdx: %v lastAppliedIdx: %v msgIdx: %v msgLog: %+v", rf.commitIndex, latestIndex, rf.LastestAppliedIndex, rf.logs[idx].Index, rf.logs[idx].Log)
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[idx].Log,
			CommandIndex: rf.logs[idx].Index,
		}
		rf.applyCh <- msg
	}

	if rf.LastestAppliedIndex > LastestAppliedIndex {
		//DPrintf("Raft %v applied log to %v", rf.me, rf.LastestAppliedIndex)
	}
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
	index := 0
	term, isLeader := rf.GetState()

	// Your code here (2B).
	if isLeader {
		index = rf.insertLog(command)
		//DPrintf("Raft %v Leader insert log to index %v at term %v", rf.me, index, term)
		rf.resetHeartbeatTimers()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	fmt.Printf("Raft %v killed\n", rf.me)
	rf.killed = true
	// Your code here, if desired.
	rf.electionTimer.Reset(0)
	rf.resetHeartbeatTimers()
}

// Vote vote function
func (rf *Raft) Vote() {
	rf.addTerm(1)
	//DPrintf("%v starts to collect vote on term %v \n", rf.me, rf.currTerm)

	logTerm, logIndex := rf.getLastLogTermAndIndex()
	req := RequestVoteArgs{
		SenderID:  rf.me,
		ElectTerm: rf.currTerm,
		LogIndex:  logIndex,
		LogTerm:   logTerm,
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

			// send request
			result := rf.sendRequestVote(idx, &req, &resp)
			//DPrintf("Raft %v get result %v", rf.me, result)

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
		rf.setTerm(term)
		rf.setRole(Follower)
	} else if grantedVote*2 > numPeers {
		fmt.Printf("New leader: %v \n", rf.me)
		rf.setRole(Leader)
		rf.resetHeartbeatTimers()
	}
}

func (rf *Raft) getLogInfo(index int, logEntries *[]Log) (preterm int, preidx int) {
	pre := index - 1
	if pre == 0 {
		preterm = 0
		preidx = 0
	} else {
		preidx = rf.logs[pre-1].Index
		preterm = rf.logs[pre-1].Term
	}

	for i := pre; i < len(rf.logs); i++ {
		*logEntries = append(*logEntries, rf.logs[i])
	}
	//DPrintf("Raft %v entries request %v", rf.me, logEntries)
	return
}

// get logs need to be appended
func (rf *Raft) getAppendLogs(server int) AppendEntriesRequest {
	rf.lock()
	defer rf.unlock()
	result := AppendEntriesRequest{
		Me:           rf.me,
		Term:         rf.currTerm,
		LeaderCommit: rf.commitIndex,
	}

	next := rf.nextIndex[server]
	result.PrevLogTerm, result.PrevLogIndex = rf.getLogInfo(next, &result.Entries)
	return result
}

func (rf *Raft) setNextIndex(server int, nextIndex int) {
	rf.lock()
	defer rf.unlock()
	rf.nextIndex[server] = nextIndex
}

func (rf *Raft) setNextAndMaxIndex(server int, maxIndex int) {
	rf.lock()
	defer rf.unlock()
	rf.nextIndex[server] = maxIndex + 1
	rf.maxIndex[server] = maxIndex
}

func (rf *Raft) replicateLogTo(server int) bool {
	replicateResult := false
	if rf.me == server {
		//DPrintf("Cannot replicate log to self")
		return replicateResult
	}

	retry := true
	for retry {
		retry = false
		currTerm, isLeader := rf.GetState()
		if !isLeader || rf.killed {
			//DPrintf("Not leader || is killed")
			break
		}
		req := rf.getAppendLogs(server)
		resp := AppendEntriesResp{}
		result := rf.sendRequestAppendEntries(server, &req, &resp)
		currTerm, isLeader = rf.GetState()
		if isLeader {
			//DPrintf("req: %v, resp: %v, result %v", req, resp, result)
		}
		if result && isLeader {
			if resp.Term > currTerm {
				// raft node too old, update term and becomes follower
				rf.setTerm(resp.Term)
				rf.setRole(Follower)
				//DPrintf("currTerm is too old")
			} else if !resp.Granted {
				rf.setNextIndex(server, resp.LastApplied+1)
				//DPrintf("set up next index of follower")
				retry = true
			} else {
				if len(req.Entries) > 0 {
					rf.setNextAndMaxIndex(server, req.Entries[len(req.Entries)-1].Index)
					replicateResult = true
					//DPrintf("leader raft %v replicate ok!", rf.me)
				}
			}
		} else {
			retry = true
		}
	}
	return replicateResult
}

// LogReplicationRoutine routine for log replication
func (rf *Raft) LogReplicationRoutine(server int) {
	defer func() {
		rf.heartbeatTimers[server].Stop()
	}()
	for !rf.killed {
		<-rf.heartbeatTimers[server].C
		if rf.killed {
			break
		}
		rf.heartbeatTimers[server].Reset(HeartbeatDuration)
		_, isLeader := rf.GetState()
		if isLeader {
			success := rf.replicateLogTo(server)
			if success {
				//DPrintf("Raft %v replicate log to %v succeeded", rf.me, server)
				rf.applyLogs()
				rf.resetHeartbeatTimers()
				rf.persist()
			}
		}
	}
}

// ElectionRoutine routine for election
func (rf *Raft) ElectionRoutine() {
	//DPrintf("Raft %v election routine launched", rf.me)
	rf.resetElectionTimer()
	defer rf.electionTimer.Stop()

	// infinite loop
	for !rf.killed {
		<-rf.electionTimer.C
		if rf.killed {
			break
		}
		if rf.getRole() == Candidate {
			rf.resetElectionTimer()
			rf.Vote()
		} else if rf.getRole() == Follower {
			rf.setRole(Candidate)
			rf.resetElectionTimer()
			rf.Vote()
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
	rf.currTerm = 0
	rf.commitIndex = 0
	rf.LastestAppliedIndex = 0
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.maxIndex = make([]int, len(rf.peers))
	rf.randTime = rand.New((rand.NewSource(time.Now().UnixNano())))
	rf.killed = false
	rf.latestLogs = AppendEntriesRequest{
		Me:   -1,
		Term: -1,
	}
	rf.electionTimer = time.NewTimer(ElectionDuration)
	rf.heartbeatTimers = make([]*time.Timer, len(rf.peers))

	rf.setRole(Follower)
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i] = time.NewTimer(HeartbeatDuration)
		go rf.LogReplicationRoutine(i)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyLogs()
	go rf.ElectionRoutine()

	fmt.Printf("Raft %v created\n", rf.me)

	return rf
}
