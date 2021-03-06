
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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

const (
	Leader    = 1
	Candidate = 2
	Follower  = 3
	// 心跳周期
	HeartBeatDuration = time.Duration(time.Millisecond * 600)
	// 选举周期
	ElectionDuration = HeartBeatDuration * 2
)

// read

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}


// lab 2B
// log entry
type LogEntry struct {
	Commond interface{}
	Index int
	Term int
}

type AppendEntryArgs struct {
	Term int
	LeaderId int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntryResp struct {
	CurrentTerm int
	Success bool
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
	// persistent state on Server
	currentTerm int
	votedFor    int
	log         []LogEntry // entry?
	// volatile state on Server
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
	//
	state int // server state
	randTime   *rand.Rand
	// election timeout
	electionTimer *time.Timer
	// lab 2b
	replicateLogTimer []*time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader := rf.state == Leader
	return rf.currentTerm, isleader

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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLongTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) ResetTimeout() {
	randtime := rf.randTime.Intn(250) // interval
	duration := time.Duration(randtime) * time.Millisecond + ElectionDuration
	rf.electionTimer.Reset(duration)
}

func (rf *Raft) getStatus() (int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) setStatus(state int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate && state == Candidate {
		rf.ResetTimeout()
	}
	rf.state = state
}

func (rf *Raft) Vote() {
	// become candidate, increase this current term, and vote
	rf.mu.Lock()
	rf.currentTerm++
	rf.mu.Unlock()
	//
	currentTerm, _ := rf.GetState()
	DPrintf("currentTerm = %+v", currentTerm)
	arg := RequestVoteArgs{
		Term : currentTerm,
		CandidateID: rf.me,
	}
	// send RequestVote RPCs to all servers
	var (
		wait sync.WaitGroup
		agreeVoted int
		term int
	)
	srvlen := len(rf.peers)
	wait.Add(srvlen)
	term = currentTerm
	for i := 0; i < srvlen; i++ {
		go func(idx int) {
			defer wait.Done()
			if idx == rf.me {
				agreeVoted++
				return
			}
			reply := RequestVoteReply{
				Term: -1,
				VoteGranted: false,
			}
			if !rf.sendRequestVote(idx, &arg, &reply) {
				DPrintf("sendRequestVote error")
				return
			}
			if reply.VoteGranted {
				agreeVoted++
				return
			}
			if term < reply.Term {
				term = reply.Term
			}
		}(i)
	}
	wait.Wait()
	// judge status
	if term > currentTerm {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.mu.Unlock()
		rf.setStatus(Follower)
	}else if agreeVoted * 2 > srvlen {
		rf.setStatus(Leader)
	}
}



func (rf *Raft) Election() {
	rf.ResetTimeout()
	defer rf.electionTimer.Stop()
	for true {
		// change role
		<-rf.electionTimer.C
		if rf.getStatus() == Candidate {
			rf.ResetTimeout() // reset election timer
			rf.Vote()
		} else if rf.getStatus() == Follower {
			rf.ResetTimeout() // reset election timer
			rf.setStatus(Candidate)
			rf.Vote()
		}

	}
}

func (rf *Raft) setNext(peer, next int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
}

func (rf *Raft) sendLogTo(peer int) (ret bool){
	ret = false
	retry := true
	for retry {
		retry = false
		curTerm, isLeader := rf.GetState()
		if !isLeader {
			break
		}
		req := AppendEntryArgs{
			Term:         curTerm,
			LeaderId:     peer,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: 0,
		}
		reply := AppendEntryResp{}
		var ok bool
		if ok = rf.sendAppendEntry(peer, &req, &reply); !ok {
			return
		}
		// 同步commitIndex to followers
		if ok && isLeader {
			// 某个节点的term打羽当前leader的term，则改变leader
			if reply.CurrentTerm > curTerm {
				rf.setTerm(reply.CurrentTerm)
				rf.setStatus(Follower)
			} else if reply.Success == false { // term < currentTerm or log doesn`t contain entry at prevLogIndex
				retry = true
				rf.setNext(peer, reply.CurrentTerm+1)
			} else { // 更新成功
				// TODO: 更新成功的操作
				ret = true

			}
		}
	}
	return
}

// replicated log
func (rf *Raft) ReplicatedLogLoop(peer int) {
	for true {
		// 当heartbeat到期后进行后续操作
		<-rf.replicateLogTimer[peer].C
		rf.mu.Lock()
		rf.replicateLogTimer[peer].Reset(HeartBeatDuration)
		rf.mu.Unlock()
		_, isLeader := rf.GetState()
		if isLeader { // if this server is leader, then send AE to followers
			ok := rf.sendLogTo(peer) // leader send log to all followers
			if !ok {
				// 如果已经送到客户端，则执行apply状态机
				rf.Apply()
				return
			}
		}

	}
	return
}


func (rf *Raft) setTerm(term int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.VoteGranted = true
	reply.Term, _ = rf.GetState()
	DPrintf("args.Term(%+v)  reply.Term(%+v)", args.Term, reply.Term)
	if args.Term < reply.Term {
		reply.VoteGranted = false
		return
	}
	// rpc request or response term > current term,set currentTerm = T, convert to follower
	rf.setStatus(Follower)
	rf.mu.Lock()
	rf.currentTerm = args.Term
	rf.mu.Unlock()
	if reply.VoteGranted {
		rf.ResetTimeout()
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
	repChan := make(chan(bool))
	ok := false
	go func () {
		rep := rf.peers[server].Call("Raft.RequestVote", args, reply)
		repChan <- rep
	}()
	select {
	case ok = <- repChan:
	case <-time.After(HeartBeatDuration):
	}
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, resp *AppendEntryResp) {

	return
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, resp *AppendEntryResp) bool {
	repChan := make(chan(bool))
	ok := false
	go func() {
		rep := rf.peers[server].Call("Raft.AppendEntry", args, resp)
		repChan <- rep
	}()
	select {
	case ok = <- repChan:
	case <-time.After(HeartBeatDuration):
	}
	return ok
}

func (rf *Raft) AddCommandToLog(command interface{})(indx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	entry := LogEntry{
		Term : rf.currentTerm,
		Commond: command,
	}
	// index
	entry.Index = rf.log[len(rf.log)].Index+1
	// append entry
	rf.log = append(rf.log, entry)
	indx = entry.Index
	return
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
func (rf *Raft) Start(command interface{}) (index int,  current_term int, is_leader bool) {
	// get current term and judge state
	current_term, is_leader = rf.GetState()
	// Your code here (2B).
	// Leader appends the command to its log
	if is_leader {
		index = rf.AddCommandToLog(command) // 更新leader的log,之后
		// leader更新定时器
		rf.mu.Lock()
		for i := 0; i < len(rf.peers); i++ {
			rf.replicateLogTimer[i].Reset(0)
		}
		rf.mu.Unlock()
	}
	return
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


// ensure the entry has been safely replicated

// apply the entry

func (rf *Raft) Apply() {

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
	rf.currentTerm = 0
	rf.state = Follower
	rf.electionTimer = time.NewTimer(ElectionDuration)
	rf.randTime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.replicateLogTimer = make([]*time.Timer, len(rf.peers)) // heartbeat Timer

	go rf.Election()

	// 当leader接收到command之后，就发送AppendEntry到每个server
	for i := 0; i < len(rf.peers); i++ {
		rf.replicateLogTimer[i] =time.NewTimer(HeartBeatDuration)
		go rf.ReplicatedLogLoop(i)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
