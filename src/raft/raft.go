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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rookie-roob/6.824/src/labgob"
	"github.com/Rookie-roob/6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

const (
	FOLLOWER             int           = 0
	CANDIDATE            int           = 1
	LEADER               int           = 2
	MIN_ELECTION_TIMEOUT int           = 200
	MAX_ELECTION_TIMEOUT int           = 350
	HEARTBEAT_INTERVAL   time.Duration = 120 * time.Millisecond
	CHECKPERIOD          int           = 10
)

/*
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
*/
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	//Snapshot relevant
	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

type LogEntry struct {
	Term       int
	LogDetails interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent State
	currentTerm int
	votedFor    int
	logs        []LogEntry

	//Volatile State
	commitIndex int
	lastApplied int
	state       int // 0 : follower; 1 : candidate; 2 : leader
	nextIndex   []int
	matchIndex  []int

	timer           *time.Ticker
	applyCh         chan ApplyMsg
	electionTimeout time.Duration
	cond            sync.Cond
	//Snapshot state
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshotCmd       []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	/*
		// Your code here (2C).
		// Example:
		// w := new(bytes.Buffer)
		// e := labgob.NewEncoder(w)
		// e.Encode(rf.xxx)
		// e.Encode(rf.yyy)
		// data := w.Bytes()
		// rf.persister.SaveRaftState(data)
	*/
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	/*
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
	*/
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var logs []LogEntry
	var votedFor int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(logs) != nil {
		fmt.Println("decode error!!!")
	} else {
		rf.currentTerm = currentTerm
		rf.logs = logs
		rf.votedFor = votedFor
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteError int64

const (
	VoteError_killed VoteError = iota
	VoteError_outofdate
	VoteError_alreadyvote
	VoteError_logtooold
	VoteError_success
)

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term      int
	VoteGrant bool
	VoteError VoteError
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		reply.VoteError = VoteError_killed
		reply.Term = -1
		reply.VoteGrant = false
		return
	}
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGrant = false
		reply.VoteError = VoteError_outofdate
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	/*
		DPrintf("args.LastLogTerm: %d", args.LastLogTerm)
		if len(rf.logs) > 0 {
			DPrintf("rf.lastterm: %d", rf.logs[len(rf.logs)-1].Term)
		}
		DPrintf("args.LastLogIndex: %d", args.LastLogIndex)
		DPrintf("rf.loglen: %d", rf.lastIncludedIndex+len(rf.logs))
	*/
	ifTermOld := args.LastLogTerm < rf.lastIncludedTerm || (len(rf.logs) > 0 && args.LastLogTerm < rf.logs[len(rf.logs)-1].Term)
	ifLogOld := args.LastLogIndex < rf.lastIncludedIndex || (len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < rf.lastIncludedIndex+len(rf.logs))
	if ifTermOld || ifLogOld {
		rf.currentTerm = args.Term // maybe args.Term > rf.currentTerm(Leader send log to Follower 1, Leader dies, Follower 2 becomes Candidate)
		rf.persist()
		reply.Term = rf.currentTerm
		reply.VoteError = VoteError_logtooold
		reply.VoteGrant = false
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm == args.Term {
		if rf.votedFor == args.CandidateId {
			// may drop previous packet
			rf.state = FOLLOWER
			rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
			rf.timer.Reset(rf.electionTimeout) //only vote for this one, then reset timer
			//DPrintf("server[%d] vote server[%d] and the voter's electiontimeout is %v", rf.me, args.CandidateId, rf.electionTimeout)
			reply.VoteError = VoteError_alreadyvote
			reply.VoteGrant = true
			rf.mu.Unlock()
			return
		}
		if rf.votedFor != -1 {
			// alreadyvote this time
			reply.Term = rf.currentTerm
			reply.VoteError = VoteError_alreadyvote
			reply.VoteGrant = false
			rf.mu.Unlock()
			return
		}
	}
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.state = FOLLOWER // may be a leader or candidate before
	rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
	rf.timer.Reset(rf.electionTimeout) // receive vote packet from candidate, need to reset timer
	//DPrintf("server[%d] vote server[%d] and the voter's electiontimeout is %v", rf.me, args.CandidateId, rf.electionTimeout)
	reply.Term = rf.currentTerm
	reply.VoteError = VoteError_success
	reply.VoteGrant = true
	rf.mu.Unlock()
}

/*
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
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, totalVoteCount *int) bool {
	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//return ok
	if rf.killed() {
		return false
	}
	for {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
	}
	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm { // another term already begin, previous request is too slow(maybe network traffic and so on)
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	if reply.VoteError == VoteError_killed {
		return false
	} else if reply.VoteError == VoteError_outofdate || reply.VoteError == VoteError_logtooold {
		rf.mu.Lock()
		rf.state = FOLLOWER
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		if reply.VoteGrant && reply.Term == rf.currentTerm && *totalVoteCount <= len(rf.peers)/2 {
			*totalVoteCount++
		}
		if *totalVoteCount > len(rf.peers)/2 {
			//DPrintf("server[%d] becomes the leader", rf.me)
			*totalVoteCount = 0
			rf.state = LEADER
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs) + rf.lastIncludedIndex + 1
			}
			rf.timer.Reset(time.Duration(HEARTBEAT_INTERVAL))
		}
		rf.mu.Unlock()
		return true
	}
	return false
}

/*
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
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return index, term, false
	}
	logEntry := LogEntry{Term: rf.currentTerm, LogDetails: command}
	//DPrintf("before append, log len: %d", len(rf.logs))
	rf.logs = append(rf.logs, logEntry)
	//DPrintf("after append, log len: %d", len(rf.logs))
	index = len(rf.logs) + rf.lastIncludedIndex
	term = rf.currentTerm
	rf.persist()
	// DPrintf("server[%d] get a command, the log index is %d, the log term is %d", rf.me, index, rf.currentTerm)
	rf.mu.Unlock()
	return index, term, isLeader
}

/*
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
*/
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.cond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//done              bool
}

type InstallSnapshotError int64

const (
	InstallSnapshotError_Success InstallSnapshotError = iota
	InstallSnapshotErr_ReqOutofDate
	InstallSnapshotErr_OldIndex
)

type InstallSnapshotReply struct {
	Term      int
	SnapError InstallSnapshotError
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		reply.Term = args.Term
		return
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.SnapError = InstallSnapshotErr_ReqOutofDate
		return
	}
	//outdate snapshot, discard it
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
		rf.timer.Reset(rf.electionTimeout)
		rf.mu.Unlock()
		reply.SnapError = InstallSnapshotErr_OldIndex
		return
	}
	//create snapshot
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
	rf.timer.Reset(rf.electionTimeout)
	if len(rf.logs)+rf.lastIncludedIndex <= args.LastIncludedIndex {
		rf.logs = []LogEntry{}
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
	} else {
		rf.logs = rf.logs[(args.LastIncludedIndex - rf.lastIncludedIndex):]
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
	}
	rf.persist()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastApplied
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.SnapError = InstallSnapshotError_Success
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}
	for {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		if ok {
			break
		}
	}
	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	if reply.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	} else if reply.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.persist()
		rf.state = FOLLOWER
		rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
		rf.timer.Reset(rf.electionTimeout)
		rf.mu.Unlock()
		return false
	} else {
		if reply.SnapError == InstallSnapshotError_Success {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		} else {
			rf.nextIndex[server] = rf.lastIncludedIndex + len(rf.logs) + 1
		}
	}
	rf.mu.Unlock()
	return true
}

type AppendEntriesError int64

const (
	AppendEntriesError_Success AppendEntriesError = iota
	AppendEntriesError_Killed
	AppendEntriesError_LogNotMatch
	AppendEntriesError_TermOutDate
	AppendEntriesError_AlreadyApplied
	AppendEntriesError_LogLenNotMatch
)

type AppendEntriesArgs struct {
	Term               int
	LeaderId           int
	PrevLogIndex       int
	PrevLogTerm        int
	Entries            []LogEntry
	LeaderCommit       int
	LeaderLogLastIndex int
}

type AppendEntriesReply struct {
	Term               int
	Success            bool
	AppendEntriesError AppendEntriesError
	ConflictIndex      int
	ConflictTerm       int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Success = false
		reply.Term = -1
		reply.AppendEntriesError = AppendEntriesError_Killed
		return
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm || (len(args.Entries) > 0 && args.PrevLogIndex < rf.lastIncludedIndex) { //request out of date, do not reset timer
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.AppendEntriesError = AppendEntriesError_TermOutDate
		rf.mu.Unlock()
		//DPrintf("server[%d] reveive an out date message from server[%d], election time out: %v", rf.me, args.LeaderId, rf.electionTimeout)
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
	rf.timer.Reset(rf.electionTimeout)
	/*
		if len(args.Entries) != 0 {
			DPrintf("server[%d] reveive a correct message from server[%d]", rf.me, args.LeaderId)
			DPrintf("args.PrevLogIndex : %d, args.PrevLogTerm: %d", args.PrevLogIndex, args.PrevLogTerm)
			if args.PrevLogIndex >= rf.lastIncludedIndex+len(rf.logs)+1 {
				DPrintf("receiver do not have such log in that index")
			} else if len(rf.logs) > 0 {
				DPrintf("receiver relevant log term: %d", rf.logs[args.PrevLogIndex-rf.lastIncludedIndex-1].Term)
			}
		}
	*/
	if args.PrevLogIndex > rf.lastIncludedIndex+len(rf.logs) {
		rf.persist()
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.AppendEntriesError = AppendEntriesError_LogLenNotMatch
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + len(rf.logs)
		rf.mu.Unlock()
		return
	}

	if (args.PrevLogIndex != rf.lastIncludedIndex && (args.PrevLogIndex >= rf.lastIncludedIndex+len(rf.logs)+1 || args.PrevLogTerm != rf.logs[args.PrevLogIndex-rf.lastIncludedIndex-1].Term)) ||
		(args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm) { // consider the impact of snapshot. args.PrevLogIndex == rf.lastIncludedIndex means that log is from the snapshot
		rf.persist()
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.AppendEntriesError = AppendEntriesError_LogNotMatch
		if (args.PrevLogIndex - rf.lastIncludedIndex - 1) >= 0 {
			reply.ConflictTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
			reply.ConflictIndex = -1
			left := rf.lastIncludedIndex + 1
			right := args.PrevLogIndex
			for left < right {
				mid := (left + right) >> 1
				if rf.logs[mid-rf.lastIncludedIndex-1].Term >= args.PrevLogTerm {
					right = mid
				} else {
					left = mid + 1
				}
			}
			reply.ConflictIndex = right
		} else {
			reply.ConflictTerm = -1
			reply.ConflictIndex = args.PrevLogIndex
		}

		rf.mu.Unlock()
		return
	}
	if args.PrevLogIndex < rf.lastApplied {
		rf.persist()
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.AppendEntriesError = AppendEntriesError_AlreadyApplied
		reply.ConflictIndex = rf.commitIndex
		reply.ConflictTerm = -1
		rf.mu.Unlock()
		return
	}

	if len(args.Entries) != 0 {
		rf.logs = rf.logs[:args.PrevLogIndex-rf.lastIncludedIndex]
		rf.logs = append(rf.logs, args.Entries...)
		//DPrintf("args.Entries last term: %d", args.Entries[len(args.Entries)-1].Term)
		//DPrintf("after append, the log's lastterm is %d", rf.logs[len(rf.logs)-1].Term)
		//DPrintf("follower append successfully")
	}
	rf.persist()
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		preCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.logs)+rf.lastIncludedIndex)))
		if preCommitIndex < rf.commitIndex {
			//wake up applyCommit
			rf.cond.Broadcast()
		}
	}
	reply.Term = rf.currentTerm
	reply.AppendEntriesError = AppendEntriesError_Success
	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) bool {
	if rf.killed() {
		return false
	}
	for {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			break
		}
	}
	if rf.killed() {
		return false
	}
	if len(args.Entries) == 0 {
		// heartbeat
		rf.mu.Lock()
		// both args and reply need to be judged
		if rf.currentTerm > args.Term {
			rf.mu.Unlock()
			return false
		}
		if rf.currentTerm > reply.Term {
			rf.mu.Unlock()
			return false
		} else if rf.currentTerm < reply.Term {
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
			rf.currentTerm = reply.Term
			rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
			rf.timer.Reset(rf.electionTimeout)
			//DPrintf("server[%d] becomes the FOLLOWER, and timeout is %v", rf.me, rf.electionTimeout)
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		if rf.currentTerm > args.Term {
			rf.mu.Unlock()
			return false
		}
		if reply.AppendEntriesError == AppendEntriesError_Killed {
			return false
		} else if reply.AppendEntriesError == AppendEntriesError_TermOutDate {
			rf.state = FOLLOWER
			//DPrintf("leader[%d] becomes the follower", rf.me)
			if rf.currentTerm < reply.Term {
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.persist()
			}
			rf.mu.Unlock()
		} else if reply.AppendEntriesError == AppendEntriesError_Success {
			if rf.nextIndex[server] > (args.LeaderLogLastIndex + 1) { // too slow reply, normally not happen
				rf.mu.Unlock()
				return false
			}
			if reply.Success && reply.Term == rf.currentTerm && *appendNum <= len(rf.peers)/2 {
				*appendNum++
			}
			rf.nextIndex[server] = (args.LeaderLogLastIndex + 1)
			if *appendNum > len(rf.peers)/2 {
				*appendNum = 0
				//outdate request
				if (rf.lastIncludedIndex < args.LeaderLogLastIndex && rf.logs[args.LeaderLogLastIndex-rf.lastIncludedIndex-1].Term != rf.currentTerm) ||
					(rf.lastIncludedIndex == args.LeaderLogLastIndex && rf.lastIncludedTerm != rf.currentTerm) { // commit rule: current term commit cause previous log entries commit. See Figure 8 in paper
					rf.mu.Unlock()
					return false
				}
				preCommitIndex := rf.commitIndex

				rf.commitIndex = rf.nextIndex[server] - 1
				// DPrintf("leader is %d, its current commitIndex is : %d", rf.me, rf.commitIndex)
				if preCommitIndex < rf.commitIndex {
					//wake up applyCommit
					rf.cond.Broadcast()
				}
			}
			rf.mu.Unlock()
		} else if reply.AppendEntriesError == AppendEntriesError_LogLenNotMatch {
			if args.Term != rf.currentTerm {
				rf.mu.Unlock()
				return false
			}
			rf.nextIndex[server] = reply.ConflictIndex + 1
			rf.mu.Unlock()
		} else if reply.AppendEntriesError == AppendEntriesError_LogNotMatch {
			if args.Term != rf.currentTerm {
				rf.mu.Unlock()
				return false
			}
			if reply.ConflictTerm == -1 {
				rf.nextIndex[server]--
			} else {
				//search reply.conflictterm
				left := rf.lastIncludedIndex + 1
				right := args.PrevLogIndex
				for left < right {
					mid := (left + right + 1) >> 1
					if rf.logs[mid-rf.lastIncludedIndex-1].Term <= reply.ConflictTerm {
						left = mid
					} else {
						right = mid - 1
					}
				}
				if rf.logs[left-rf.lastIncludedIndex-1].Term == reply.ConflictTerm {
					rf.nextIndex[server] = left + 1
				} else {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}
			rf.mu.Unlock()
		} else {
			if args.Term != rf.currentTerm {
				rf.mu.Unlock()
				return false
			}
			rf.nextIndex[server] = reply.ConflictIndex + 1
			rf.mu.Unlock()
		}
	}
	return true
}

// check if time out
func (rf *Raft) ticker() {
	for {
		if rf.killed() {
			break
		}
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.state {
			case FOLLOWER:
				rf.state = CANDIDATE
				fallthrough
			case CANDIDATE:
				//DPrintf("server[%d] becomes the candidate", rf.me)
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()
				rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
				rf.timer.Reset(rf.electionTimeout)
				//DPrintf("server[%d] is the candidate, electiontimeout is %v", rf.me, rf.electionTimeout)
				totalVoteCount := 1
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					voteArgs := RequestVoteArgs{}
					voteArgs.CandidateId = rf.me
					voteArgs.LastLogIndex = rf.lastIncludedIndex + len(rf.logs)
					voteArgs.Term = rf.currentTerm // do not forget to set the term and other members in voteArgs
					if len(rf.logs) == 0 {
						voteArgs.LastLogTerm = rf.lastIncludedTerm
					} else {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}
					voteResp := RequestVoteReply{}
					go rf.sendRequestVote(i, &voteArgs, &voteResp, &totalVoteCount)
				}
			case LEADER:
				// heartbeat interval setting
				rf.timer.Reset(HEARTBEAT_INTERVAL)
				appendNum := 1
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					//send snapshot
					if rf.nextIndex[i] <= rf.lastIncludedIndex {
						installSnapshortArgs := &InstallSnapshotArgs{
							Term:              rf.currentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.lastIncludedIndex,
							LastIncludedTerm:  rf.lastIncludedTerm,
							Data:              rf.snapshotCmd,
						}
						installSnapshortReply := &InstallSnapshotReply{}
						go rf.sendInstallSnapshot(i, installSnapshortArgs, installSnapshortReply)
					}
					appendEntriesArgs := &AppendEntriesArgs{
						Term:               rf.currentTerm,
						LeaderId:           rf.me,
						PrevLogIndex:       0,
						PrevLogTerm:        0,
						Entries:            nil,
						LeaderCommit:       rf.commitIndex,
						LeaderLogLastIndex: rf.lastIncludedIndex + len(rf.logs),
					}
					for rf.nextIndex[i] > rf.lastIncludedIndex {
						appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
						if appendEntriesArgs.PrevLogIndex >= len(rf.logs)+rf.lastIncludedIndex+1 {
							rf.nextIndex[i]--
							continue
						}
						if appendEntriesArgs.PrevLogIndex == rf.lastIncludedIndex {
							appendEntriesArgs.PrevLogTerm = rf.lastIncludedTerm
						} else {
							appendEntriesArgs.PrevLogTerm = rf.logs[appendEntriesArgs.PrevLogIndex-rf.lastIncludedIndex-1].Term
						}
						break
					}
					if appendEntriesArgs.PrevLogIndex < len(rf.logs)+rf.lastIncludedIndex {
						appendEntriesArgs.Entries = make([]LogEntry, len(rf.logs)+rf.lastIncludedIndex-appendEntriesArgs.PrevLogIndex) // append log length must be correct !!! because next step is copy, copy src and dst' length must be the same
						copy(appendEntriesArgs.Entries, rf.logs[appendEntriesArgs.PrevLogIndex-rf.lastIncludedIndex:len(rf.logs)])
					}
					appendEntriesReply := &AppendEntriesReply{}
					go rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply, &appendNum)
				}
			}
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) applychecker() {
	for {
		rf.mu.Lock()
		/*
			if rf.killed() {
				close(rf.applyCh)
				return
			}
		*/
		for rf.commitIndex <= rf.lastApplied {
			/*
				if rf.killed() {
					close(rf.applyCh)
					return
				}
			*/
			rf.cond.Wait()
		}
		/*
			if rf.killed() {
				close(rf.applyCh)
				return
			}
		*/
		rf.lastApplied++
		commitIndex := rf.lastApplied // should commit in index order
		command := rf.logs[commitIndex-rf.lastIncludedIndex-1].LogDetails
		rf.mu.Unlock()
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: commitIndex,
		}
		rf.applyCh <- applyMsg
	}
}

/*
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{} //cannot write []LogEntry{0,nil} whitch means that rf.logs[0] = 0,so the len will be 1!!!!
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.cond = *sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
	rf.timer = time.NewTicker(rf.electionTimeout)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapshotCmd = make([]byte, 0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applychecker()
	return rf
}
