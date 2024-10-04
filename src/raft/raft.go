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

type ApplyMsg struct {
	CommandValid bool // 为false的话表示为快照消息
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	//Snapshot relevant
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

	appendCh chan bool
	voteCh   chan bool
	killCh   chan bool
	//Snapshot state
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshotData      []byte
	snapshotIndex     int
	snapshotTerm      int
}

func (rf *Raft) beFollower(currentTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = currentTerm
	rf.votedFor = -1
	rf.persist()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
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
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var logs []LogEntry
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int // 在有snapshot的情况下，lastIncludedIndex以及lastIncludedTerm也要持久化，不然reboot后根本就不知道日志index开始点
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil { // 传切片的引用和传切片意义不同
		fmt.Println("decode error!!!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.logs = logs
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
		rf.mu.Unlock()
	}
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
	//DPrintf("server[%d] get a command, the log index is %d, the log term is %d", rf.me, index, rf.currentTerm)
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
	rf.mu.Unlock()
	rf.cond.Broadcast()
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
	rf.lastApplied = Max(rf.lastIncludedIndex, rf.lastApplied)
	rf.commitIndex = Max(rf.lastIncludedIndex, rf.commitIndex) //注意这里要取最大值，因为可能此时receiver的日志也在涨的！！！
	rf.snapshotData = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.SnapError = InstallSnapshotError_Success
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}
	for {
		if rf.killed() {
			return false
		}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		if ok {
			break
		}
		if rf.killed() {
			return false
		}
	}
	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	if reply.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	} else if reply.SnapError == InstallSnapshotErr_OldIndex {
		if reply.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.persist()
			rf.state = FOLLOWER
			rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
			rf.timer.Reset(rf.electionTimeout)
		}
		rf.nextIndex[server] = rf.lastIncludedIndex + len(rf.logs) + 1
	} else if reply.SnapError == InstallSnapshotError_Success {
		if reply.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.persist()
			rf.state = FOLLOWER
			rf.electionTimeout = time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
			rf.timer.Reset(rf.electionTimeout)
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
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

// check if time out
func (rf *Raft) ticker() {
	for rf.killed() == false {
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
							Data:              rf.persister.ReadSnapshot(),
						}
						installSnapshortReply := &InstallSnapshotReply{}
						go rf.sendInstallSnapshot(i, installSnapshortArgs, installSnapshortReply)
						continue
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
					//DPrintf("rf.nextIndex[%v]: %v", i, rf.nextIndex[i])
					//DPrintf("send to server[%v], appendEntriesArgs.PrevLogIndex: %v, len(appendEntriesArgs.Entries): %v", i, appendEntriesArgs.PrevLogIndex, len(appendEntriesArgs.Entries))
					go rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply, &appendNum)
				}
			}
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) applychecker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		for rf.commitIndex <= rf.lastApplied && rf.snapshotData == nil {
			rf.cond.Wait()
		}
		if rf.snapshotData != nil {
			applyMsg := ApplyMsg{
				CommandValid:  false,
				Snapshot:      rf.snapshotData,
				SnapshotIndex: rf.snapshotIndex,
				SnapshotTerm:  rf.snapshotTerm,
			}
			rf.snapshotData = nil
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.lastApplied++
			commitIndex := rf.lastApplied // should commit in index order
			//DPrintf("applychecker: rf.me:%v, len(rf.logs):%v, rf.state==Leader: %v", rf.me, len(rf.logs), rf.state == LEADER)
			//DPrintf("applychecker: rf.me:%v, commitIndex: %v, rf.lastIncludedIndex: %v", rf.me, commitIndex, rf.lastIncludedIndex)
			command := rf.logs[commitIndex-rf.lastIncludedIndex-1].LogDetails
			commandTerm := rf.logs[commitIndex-rf.lastIncludedIndex-1].Term // 在这里同步，把值都安全地取出来，因为logs是可能在appendEntries中被修改的！！！
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandTerm:  commandTerm,
				CommandIndex: commitIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}

	}
}

/*
**go中类的方法和成员的可见性都是根据其首字母的大小写来决定的，
**如果变量名、属性名、函数名或方法名首字母大写，就可以在包外直接访问这些变量、属性、函数和方法，否则只能在包内访问，
**因此 Go 语言类属性和成员方法的可见性都是包一级的，而不是类一级的。
 */
func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) service() {

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
	rf.logs = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.applyCh = applyCh

	rf.voteCh = make(chan bool, 1)
	rf.appendCh = make(chan bool, 1)
	rf.killCh = make(chan bool, 1)
	rf.snapshotData = nil
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.service()
	return rf
}

//Snapshot 相关，这个是主动做snapshot（server层检查到log太大，向raft层发出的snapshot）

func (rf *Raft) persistStateMachineAndRaftState(kvdata []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, kvdata)
}

func (rf *Raft) DoSnapshot(logIndex int, kvdata []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if logIndex <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedTerm = rf.logs[logIndex-rf.lastIncludedIndex-1].Term
	rf.logs = append(make([]LogEntry, 0), rf.logs[logIndex-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = logIndex
	rf.persistStateMachineAndRaftState(kvdata)
}

func (rf *Raft) updateMatchIndex(server int, matchIndex int) {
	rf.matchIndex[server] = matchIndex
	rf.nextIndex[server] = matchIndex + 1
	rf.updateCommitIndex()
}

// commit logs
func (rf *Raft) Commit() {
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		command := rf.logs[rf.lastApplied-rf.lastIncludedIndex].LogDetails
		commandTerm := rf.logs[rf.lastApplied-rf.lastIncludedIndex].Term
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandTerm:  commandTerm,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.lastIncludedIndex + len(rf.logs) - 1
	// if there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm, set commitIndex = N
	for n := rf.matchIndex[rf.me]; n >= rf.commitIndex; n-- {
		count := 1
		if rf.logs[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		} else if rf.logs[n].Term < rf.currentTerm {
			break
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.Commit()
			break
		}
	}
}

func (rf *Raft) getLastLogTerm() int {
	lastIdx := rf.lastIncludedIndex + len(rf.logs) - 1
	if lastIdx < rf.lastIncludedIndex {
		return -1
	}
	return rf.logs[lastIdx-rf.lastIncludedIndex].Term
}

func (rf *Raft) beLeader() {
	if rf.state != CANDIDATE {
		return
	}
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	initial_index := rf.lastIncludedIndex + len(rf.logs)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = initial_index
	}
}
