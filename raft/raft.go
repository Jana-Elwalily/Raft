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
	Command interface{} //actuall operation requested
	Term    int         // leader that created that entry
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
	//3b fields
	log         []LogEntry
	commitIndex int //entry is committed
	lastApplied int //entry alr delivered to service

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

// Raft server states
const (
	Follower = iota
	Candidate
	Leader
)

const (
	heartbeatInterval = 100 * time.Millisecond
	electionMin       = 150 * time.Millisecond
	electionMax       = 300 * time.Millisecond
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

func (rf *Raft) lastLogIndexLocked() int {
	return len(rf.log) - 1
}
func (rf *Raft) lastLogTermLocked() int {
	return rf.log[len(rf.log)-1].Term
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
	// Compare the candidate's log with our own
	myLastIndex := rf.lastLogIndexLocked()
	myLastTerm := rf.lastLogTermLocked()

	logIsUpToDate := args.LastLogTerm > myLastTerm || (args.LastLogTerm == myLastTerm &&
		args.LastLogIndex >= myLastIndex)

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
	//when Success is false
	ConflictTerm  int
	ConflictIndex int
}

// Must be called while holding rf.mu.
func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader

	lastIndex := rf.lastLogIndexLocked()

	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}

	// The leader already has its own log entries.
	rf.matchIndex[rf.me] = lastIndex
	rf.nextIndex[rf.me] = lastIndex + 1

	// Trigger an immediate heartbeat/replication round.
	rf.lastHeartbeat = time.Time{}
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
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0
	// Reject an outdated leader.
	if args.Term < rf.currentTerm {
		return
	}

	// A valid leader with an equal or higher term forces this
	// server to become a follower.
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	} else {
		rf.state = Follower

	}
	rf.resetElectionDeadlineLocked()
	reply.Term = rf.currentTerm
	if args.PrevLogIndex < 0 {
		return
	}
	// Check whether PrevLogIndex exists in our log.
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log)
		return
	}

	// Check whether the previous log entry has the expected term.
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		ct := rf.log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex
		for ci > 1 && rf.log[ci-1].Term == ct {
			ci--
		}
		reply.ConflictTerm = ct
		reply.ConflictIndex = ci
		return
	}

	// Merge incoming entries with our log.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i

		if index < len(rf.log) {
			// Keep entries that already match.
			if rf.log[index].Term == entry.Term {
				continue
			}

			// Never overwrite an entry that is already committed.
			if index <= rf.commitIndex {
				return
			}

			// Removing the conflicting entry and everything after it.
			rf.log = rf.log[:index]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}

		// Our log is shorter, so append the remaining entries
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)

		newCommitIndex := args.LeaderCommit
		if newCommitIndex > lastNewIndex {
			newCommitIndex = lastNewIndex
		}

		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
		}
	}
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
	//index := -1
	//term := -1
	//isLeader := false

	// Your code here (3B).
	rf.mu.Lock()

	if rf.killed() || rf.state != Leader {
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}

	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Command: command,
		Term: term})
	index := rf.lastLogIndexLocked()
	// Update the leader's own replication progress.
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.updateCommitIndexLocked()
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.replicateToPeer(peer)
		}
	}

	return index, term, true
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

	// Do not start an election if killed or already leader.
	if rf.killed() || rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	// Start a new election term.
	rf.state = Candidate
	rf.currentTerm++
	term := rf.currentTerm

	// Vote for ourselves.
	rf.votedFor = rf.me
	votesReceived := 1

	// Reset election timeout.
	rf.resetElectionDeadlineLocked()
	timeout := time.Until(rf.electionDeadline)

	// Include our actual latest log information.
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndexLocked(),
		LastLogTerm:  rf.lastLogTermLocked(),
	}

	majority := len(rf.peers)/2 + 1

	// A single-node cluster elects itself immediately.
	if votesReceived >= majority {
		rf.becomeLeaderLocked()
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	// Each peer reports whether its RPC succeeded and its reply.
	type voteResult struct {
		reply RequestVoteReply
		ok    bool
	}

	voteCh := make(chan voteResult, len(rf.peers)-1)

	// Request votes from all other peers.
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply

			ok := rf.sendRequestVote(server, &args, &reply)

			voteCh <- voteResult{
				reply: reply,
				ok:    ok,
			}
		}(peer)
	}

	// Use one timer for the entire election.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	received := 0

	for received < len(rf.peers)-1 {
		select {

		case result := <-voteCh:
			received++
			reply := result.reply

			rf.mu.Lock()

			// A higher term means we must become a follower.
			if result.ok && reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.mu.Unlock()
				return
			}

			// Ignore responses from an old election.
			if rf.currentTerm != term || rf.state != Candidate {
				rf.mu.Unlock()
				return
			}

			// Count only valid votes for this election term.
			if result.ok &&
				reply.Term == term &&
				reply.VoteGranted {

				votesReceived++

				if votesReceived >= majority {
					rf.becomeLeaderLocked()
					rf.mu.Unlock()
					return
				}
			}

			// Even if all remaining peers vote for us,
			// we can no longer win.
			remaining := len(rf.peers) - 1 - received

			if votesReceived+remaining < majority {
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()

		case <-timer.C:
			// Election timed out. The ticker can start another election.
			return
		}
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
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.dead = 0

	// Initialize the log with a dummy entry at index 0.
	rf.log = []LogEntry{
		{
			Command: nil,
			Term:    0,
		},
	}

	// Initialize commit and application indexes.
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Initialize replication progress.
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := range peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// Initialize election timeout.
	rf.mu.Lock()
	rf.resetElectionDeadlineLocked()
	rf.mu.Unlock()

	// Start background goroutines.
	go rf.ticker()
	go rf.applier()

	return rf
}

// new
func (rf *Raft) updateCommitIndexLocked() {
	if rf.state != Leader {
		return
	}
	lastIndex := rf.lastLogIndexLocked()
	majority := len(rf.peers)/2 + 1

	// Check entries from newest to oldest.
	for n := lastIndex; n > rf.commitIndex; n-- {

		// Raft can directly commit an entry from its current term.
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		replicatedCount := 0

		// Count servers that have replicated this entry.
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= n {
				replicatedCount++
			}
		}

		// Commit if a majority has replicated this entry.
		if replicatedCount >= majority {
			rf.commitIndex = n
			return
		}
	}
}
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me {
			go rf.replicateToPeer(peer)
		}
	}
}

// new
func (rf *Raft) replicateToPeer(peer int) {
	for !rf.killed() {
		rf.mu.Lock()

		// Stop not leader.
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		term := rf.currentTerm
		next := rf.nextIndex[peer]

		// Keep nextIndex within valid bounds
		if next < 1 {
			next = 1
		}

		lastIndex := rf.lastLogIndexLocked()

		if next > lastIndex+1 {
			next = lastIndex + 1
		}
		rf.nextIndex[peer] = next
		prevIndex := next - 1
		prevTerm := rf.log[prevIndex].Term
		//copy entries
		entries := make([]LogEntry, lastIndex-next+1)
		copy(entries, rf.log[next:lastIndex+1])

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		var reply AppendEntriesReply

		ok := rf.sendAppendEntries(peer, &args, &reply)

		if !ok {
			// Retry during a later heartbeat or Start().
			return
		}

		rf.mu.Lock()

		//newer term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			rf.mu.Unlock()
			return
		}

		// Ignore responses belonging to an old leadership term.
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			newMatch := prevIndex + len(entries)

			// Never move matchIndex backwards (replies can be reordered).
			if newMatch > rf.matchIndex[peer] {
				rf.matchIndex[peer] = newMatch
			}
			if rf.nextIndex[peer] < rf.matchIndex[peer]+1 {
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			}

			rf.updateCommitIndexLocked()
			rf.mu.Unlock()
			return
		}
		//log mismatch
		if rf.nextIndex[peer] != next {
			rf.mu.Unlock()
			return
		}
		newNext := reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			// If we have entries of the conflicting term
			for i := rf.lastLogIndexLocked(); i > 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					newNext = i + 1
					break
				}
				if rf.log[i].Term < reply.ConflictTerm {
					break
				}
			}
		}
		if newNext >= next {
			newNext = next - 1
		}
		if newNext < 1 {
			newNext = 1
		}
		rf.nextIndex[peer] = newNext

		rf.mu.Unlock()
		// loop and retry immediately with the corrected nextIndex

	}
}
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		index := rf.lastApplied + 1
		entry := rf.log[index]

		msg := ApplyMsg{
			Index:   index,
			Command: entry.Command,
		}

		rf.mu.Unlock()

		rf.applyCh <- msg

		rf.mu.Lock()

		if rf.lastApplied < index {
			rf.lastApplied = index
		}

		rf.mu.Unlock()
	}
}
