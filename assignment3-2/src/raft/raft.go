package raft
import (
	//"fmt"
	"time"
	"math/rand"
)
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

import "sync"
import "labrpc"

// import "bytes"
// import "encoding/gob"

const MIN_TIMEOUT_VALUE = 150
const MAX_TIMEOUT_VALUE = 300
const VOTING_TIMEOUT_VALUE = MIN_TIMEOUT_VALUE + MAX_TIMEOUT_VALUE + 500

const (
	FOLLOWER = iota
	CANDIDATE = iota
	LEADER = iota
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// RPC struct needed in the sender and handler.
//
type AppendEntries struct {
	Term            int   // Leaders term
	LeaderId        int   // Leaders Id
	PrevLogIndex    int   // index of log entry immediately proceding new ones
	PrevLogTerm     int   // term of PrevLogIndex entry
	Entries         []int // log entries to store (empty for hearbeat)
	LeadersCommit   int   // leader's commitIndex
}

//
// RPC struct needed in the receiver of the RPC to
//
type AppendEntriesReply struct {
	Term     int  // Raft.currentTerm, for leader to update itself
	Success  bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

//
// The struct to handle the heartbet timeouts
//
type RaftTimeout struct {
	timeout        time.Duration  // The duration of the timeout
	snapshot       time.Time      // The current(last) time the hearbeat was received
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu               sync.Mutex
	peers            []*labrpc.ClientEnd
	persister        *Persister
	me               int      // index into peers[]

	// Persistent data
	currentTerm      int
	votedFor         int      // candidateId. null (-1) if none
	log              []int    //log entries. Command for state machine first is 1

	// Volatile data
	commitIndex      int
	lastApplied      int

	// Volatile only on Leader
	nextIndex        []int    // for each server, index of the next log entry
	matchIndex       []int    // for each server, index of highest log entry

	// Our own data
	state            int
	electionTimeout  RaftTimeout
	votingTimeout    RaftTimeout
	leaderHeartbeat  RaftTimeout
	currentVotes     int
	applyChannel     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}


//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term            int
	CandidateId     int
	LastLogIndex    int
	LastLogTerm     int
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term          int  // this.currentTerm, for candidate to update itself
	VoteGranted   bool // true means candidate received vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if (args.Term < rf.currentTerm) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if (args.Term > rf.currentTerm) {
		// It updates the current term with the highest one
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.electionTimeout.snapshot = time.Now()
		rf.state = FOLLOWER

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		DPrintf("RequestVote:: Term > than. Server %d Votes %d\n", rf.me, args.CandidateId)
	} else {
		if ((args.LastLogIndex >= rf.lastApplied) &&
			(rf.votedFor == -1)) {

			rf.votedFor = args.CandidateId
			rf.electionTimeout.snapshot = time.Now()
			rf.state = FOLLOWER

			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			DPrintf("RequestVote:: Server %d Votes %d\n", rf.me, args.CandidateId)
		} else {
			reply.VoteGranted = false
			DPrintf("RequestVote::I(%d) have already voted or you are outdated\n", rf.me)
		}
	}
	rf.mu.Unlock()
}

//
// Send a RequestVote RPC to a server.
// returns true if labrpc says the RPC was delivered.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("sendRequestVote:: to serverId %d from:%d\n", server, rf.me)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// HeartbeatReceived RPC handle
//
func (rf *Raft) HeartbeatReceived(entry AppendEntries, reply *AppendEntriesReply) {
	DPrintf("HeartbeatReceived::Received by server Nbr %d from: %d\n", rf.me, entry.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionTimeout.snapshot = time.Now()

	if (entry.Term < rf.currentTerm) {
		reply.Success = false
	}
	rf.votedFor = -1
	if (entry.Term > rf.currentTerm) {
		DPrintf("Updating the currentTerm\n")
		rf.currentTerm = entry.Term
		rf.state = FOLLOWER
	}
}

//
// Sends a HeartbeatReceived RPC to a server
// returns true if labrpc says the RPC was delivered
//
func (rf *Raft) sendHeartbeat(server int, entry AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HeartbeatReceived", entry, reply)

	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("Raft Start")
	rf.mu.Lock()
	index := rf.commitIndex
	term := rf.currentTerm
	leader := rf.state == LEADER
	rf.mu.Unlock()


	return index, term, leader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("Server %d has been killed\n", rf.me)
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

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]int, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = FOLLOWER
	rf.currentVotes = 0
	rf.applyChannel = applyCh

	// Uses the current time to calculated the seed -> Random timeout value
	seconds := time.Now().Second()
	newSource := rand.NewSource(int64(seconds * (rf.me + 1)))
	timeoutValue := rand.New(newSource).Int() %
						(MAX_TIMEOUT_VALUE - MIN_TIMEOUT_VALUE)
	duration := time.Duration(MIN_TIMEOUT_VALUE + timeoutValue) * time.Millisecond

	currTime := time.Now()
	rf.electionTimeout = RaftTimeout{duration, currTime}
	rf.votingTimeout = RaftTimeout{VOTING_TIMEOUT_VALUE, currTime}
	rf.leaderHeartbeat = RaftTimeout{duration / 2, currTime}

	go HandleRaftState(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}


func HandleRaftState(rf *Raft) {
	for {
		switch rf.state {
			case FOLLOWER:
				rf.mu.Lock()
				timeDiff := time.Since(rf.electionTimeout.snapshot)
				valueElectionTimeout := rf.electionTimeout.timeout
				rf.mu.Unlock()
				// We have not received the heartbeat
				// We are going to request a vote
				if (timeDiff >= valueElectionTimeout) {
					handleVoteRequest(rf)
				}
			case CANDIDATE:
				rf.mu.Lock()
				timeDiff := time.Since(rf.votingTimeout.snapshot)
				valueVotingTimeout := rf.votingTimeout.timeout
				server := rf.me
				rf.mu.Unlock()

				// We are still in CANDIDATE and the voting was not conclusive
				if (timeDiff >= valueVotingTimeout) {
					DPrintf("The time in Candidate has passed for server: %d\n", server)
					handleVoteRequest(rf)
				}

			case LEADER:
				rf.mu.Lock()
				timeDiff := time.Since(rf.leaderHeartbeat.snapshot)
				valueHeartbeatTimeout := rf.leaderHeartbeat.timeout
				rf.mu.Unlock()

				if (timeDiff >= valueHeartbeatTimeout) {
					handleHeartbeat(rf)
				}
		}
		time.Sleep(10 * time.Millisecond)
	}
}


func handleVoteRequest(rf *Raft) {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.state = CANDIDATE
	rf.votingTimeout.snapshot = time.Now()
	rf.votedFor = rf.me
	rf.currentVotes = 1
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		// Do not send the vote request to myself
		if (i != rf.me) {
			go sendVoteRequestsRoutine(rf, i)
		}
	}
}

func sendVoteRequestsRoutine(rf *Raft, server int) {
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastApplied
	args.LastLogTerm = rf.currentTerm

	reply := &RequestVoteReply{-1, false}

	result := rf.sendRequestVote(server, args, reply)

	if (result) {
		rf.mu.Lock()
		if (reply.VoteGranted == true) {
			rf.currentVotes += 1
		} else {
			rf.currentTerm = reply.Term
		}
		if (rf.currentVotes > ((len(rf.peers)) / 2)) {
			// Don't initialize the timer here so the new leader can immediately
			// send the heartbeat to the FOLLOWERs/CANDIDATEs
			rf.state = LEADER
			DPrintf("I won the election: %d\n", rf.me)
		}
		rf.mu.Unlock()
	} else {
		DPrintf("sendRequestsRoutine:: Error al mandar el mensaje\n")
	}
}

func handleHeartbeat(rf *Raft) {
	rf.mu.Lock()
	rf.leaderHeartbeat.snapshot = time.Now()

	for i := 0; i < len(rf.peers); i++ {
		// Do not send the vote request to myself
		if (i != rf.me) {
			go sendHeartbeatRoutine(rf, i)
		}
	}
	rf.mu.Unlock()
}

func sendHeartbeatRoutine(rf *Raft, server int) {
	args := AppendEntries{}
	reply := &AppendEntriesReply{-1, false}

	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.lastApplied
	args.PrevLogTerm = rf.currentTerm - 1 // ?
	args.Entries = make([]int, 0)  // Sendint empy entries wwhan sending heartbeat
	args.LeadersCommit = rf.commitIndex
	rf.mu.Unlock()

	result := rf.sendHeartbeat(server, args, reply)

	if (!result) {
		DPrintf("sendHeartbeatRoutine:: Error al mandar el mensaje\n")
	}
}


