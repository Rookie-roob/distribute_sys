package raft

import "sync/atomic"

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term      int
	VoteGrant bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGrant = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	reply.VoteGrant = false
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// if votedFor is not null and not candidateId, voted already
		rf.persist()
	} else if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < (rf.lastIncludedIndex+len(rf.logs)-1)) {
		// logs not up to date
		rf.persist()
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGrant = true
		rf.state = FOLLOWER
		rf.persist()
		sendToChan(rf.voteCh)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastVoteReq() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastIncludedIndex + len(rf.logs) - 1,
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	totalVoteCount := int32(1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(idx, &args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.persist()
					return
				}
				if rf.state != CANDIDATE || rf.currentTerm != args.Term {
					return
				}
				if reply.VoteGrant {
					atomic.AddInt32(&totalVoteCount, 1)
				}
				if atomic.LoadInt32(&totalVoteCount) > int32(len(rf.peers)/2) {
					rf.beLeader()
					rf.broadcastHeartbeat()
					sendToChan(rf.voteCh)
				}
			}
		}(i)
	}
}
