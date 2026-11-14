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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"lab2raft/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // Reserved for a later lab; ignore in Lab 2.
	Snapshot    []byte // Reserved for a later lab; ignore in Lab 2.
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Persistent Raft state
	currentTerm int
	votedFor    int

	state int // Volatile Raft state

	electionDeadline time.Time // Election timeout management

	lastHeartbeat time.Time // Leader heartbeat management

	dead int32 // Shutdown flag

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// Raft server states
const (
	Follower = iota
	Candidate
	Leader
)

const (
	heartbeatInterval = 100 * time.Millisecond
	electionMin       = 300 * time.Millisecond
	electionMax       = 500 * time.Millisecond
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm // Get the current term

	isleader = rf.state == Leader // Check whether this peer is the leader

	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Initialize reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Reject requests from older terms
	if args.Term < rf.currentTerm {
		return
	}

	// A higher term means we must become a follower
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// The candidate is considered up to date
	var logIsUpToDate bool
	logIsUpToDate = true

	if logIsUpToDate &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {

		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionDeadlineLocked()

		reply.VoteGranted = true
	}

	// Return the current term
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (3A, 3B).
	Term    int
	Success bool
}

// Reset the election timeout.
// Must be called while holding rf.mu.
func (rf *Raft) resetElectionDeadlineLocked() {
	var timeout time.Duration

	// Generate a random election timeout
	timeout = electionMin + time.Duration(rand.Int63n(int64(electionMax-electionMin)))

	// Set the election deadline
	rf.electionDeadline = time.Now().Add(timeout)
}

func (rf *Raft) becomeFollowerLocked(term int) {
	// Update the term if the received term is newer
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}

	// Change the state to follower
	rf.state = Follower

	// Reset the election timeout
	rf.resetElectionDeadlineLocked()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Initialize the reply
	reply.Term = rf.currentTerm
	reply.Success = false

	// Reject requests from older terms
	if args.Term < rf.currentTerm {
		return
	}

	// A valid leader's heartbeat prevents an election
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	} else {
		rf.state = Follower
		rf.resetElectionDeadlineLocked()
	}

	// Accept the heartbeat
	reply.Term = rf.currentTerm
	reply.Success = true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.dead, 1)
}

// Check whether this Raft instance has been killed
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// Start a new election
func (rf *Raft) startElection() {
	rf.mu.Lock()

	// Only a follower or candidate may start an election
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	rf.state = Candidate
	rf.currentTerm++
	term := rf.currentTerm

	rf.votedFor = rf.me
	rf.resetElectionDeadlineLocked()

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	votesReceived := 1
	majority := len(rf.peers)/2 + 1

	rf.mu.Unlock()

	// A single-node cluster elects itself immediately
	if votesReceived >= majority {
		rf.mu.Lock()

		if rf.currentTerm == term &&
			rf.state == Candidate {

			rf.state = Leader
			rf.lastHeartbeat = time.Time{}
		}

		rf.mu.Unlock()
		return
	}

	// Request votes from all other peers
	voteCh := make(chan RequestVoteReply, len(rf.peers))

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply

			ok := rf.sendRequestVote(server, &args, &reply)

			if ok {
				voteCh <- reply
			}
		}(peer)
	}

	// Collect votes until we win, lose or election deadline expires
	for received := 0; received < len(rf.peers)-1; received++ {
		select {
		case reply := <-voteCh:
			rf.mu.Lock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.mu.Unlock()
				return
			}

			// Ignore votes from a stale election
			if rf.currentTerm != term ||
				rf.state != Candidate {
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votesReceived++

				if votesReceived >= majority {
					rf.state = Leader
					rf.lastHeartbeat = time.Time{}
					rf.mu.Unlock()
					return
				}
			}

			rf.mu.Unlock()

		case <-time.After(electionMax):
			return
		}
	}
}

// Send heartbeat messages to all peers
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {
			var reply AppendEntriesReply

			ok := rf.sendAppendEntries(
				server,
				&args,
				&reply,
			)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// A newer term means the leader must step down
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
			}
		}(peer)
	}
}

func (rf *Raft) ticker() {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for !rf.killed() {
		<-tick.C

		rf.mu.Lock()

		state := rf.state
		now := time.Now()

		// Leaders send heartbeats every 100 ms(10 heartbeats per second)
		if state == Leader {
			if now.Sub(rf.lastHeartbeat) >= heartbeatInterval {
				rf.lastHeartbeat = now
				rf.mu.Unlock()

				rf.sendHeartbeats()
				continue
			}

			rf.mu.Unlock()
			continue
		}

		// Followers and candidates start an election when their election deadline expires
		if now.After(rf.electionDeadline) {
			rf.mu.Unlock()

			rf.startElection()
			continue
		}

		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower

	rf.dead = 0

	rf.resetElectionDeadlineLocked()

	// Start the election/heartbeat loop
	go rf.ticker()

	return rf
}
