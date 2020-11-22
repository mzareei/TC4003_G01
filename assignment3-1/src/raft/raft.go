package raft
import (
	"fmt"
	"time"
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
type AppendEntries struct {
	// Empty for the moment, only to be used in the Heartbeat
}

type AppendEntriesReply struct {
	// Empty for the moment, only to be used in the Hearbeat
	dummy int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent data
	currentTerm      int
	votedFor         int        // candidateId. null (-1) if none
	log              []string   //log entries. Command for state machine first is 1

	// Volatile data
	commitIndex      int
	lastApplied      int

	// Volatile only on Leader
	nextIndex        []int    // for each server, index of the next log entry
	matchIndex       []int    // for each server, index of highest log entry

	// Our own data
	isLeader         bool
	currentTime      *time.Timer
	applyChannel	chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.isLeader
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
	term            int
	candidateId     int
	lastLogIndex    int
	lastLogTerm     int
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	term          int  // this.currentTerm, for candidate to update itself
	voteGranted   bool // true means candidate received vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("RequestVote received. I'm %d\n", rf.me)
	if (args.term < rf.currentTerm) {
		reply.voteGranted = false
	} else {
		if ((rf.lastApplied <= args.lastLogIndex) &&
		   (rf.votedFor != -1)) {
			reply.voteGranted = true
		}
	}
	// not right
	reply.term = rf.currentTerm
	reply.voteGranted = true
	return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("sendRequestVote from serverId %d\n", rf.me)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



func (rf *Raft) HeartbeatReceived(entry AppendEntries, reply *AppendEntriesReply) {
	fmt.Printf("Heartbeat received by server Nbr %d\n", rf.me)
}


func (rf *Raft) sendHeartbeat(entry AppendEntries, reply *AppendEntriesReply) bool {
	fmt.Printf("Sending Heartbeat from server %d\n", rf.me)
	var result = true
	for i := 0; i < len(rf.peers); i++ {
		// Do not send the heartbeat to myself
		if (i != rf.me) {
			fmt.Printf("");
			result = rf.peers[i].Call("Raft.HeartbeatReceived", entry, &reply)
		}
	}
	return result
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
	fmt.Printf("Raft Start")
	index := -1
	term := -1
	isLeader := true


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
	rf.log = make([]string, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.isLeader = false
	rf.applyChannel = applyCh

	/*rf.currentTime = time.AfterFunc(500 * time.Millisecond, func() {
		arg := AppendEntries{}
		reply := AppendEntriesReply{}
		result := rf.sendHeartbeat(arg, &reply)
		if (!result) {
			fmt.Println("Error sending the Heartbeat")
		}
	})*/

	go startLeaderVote(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}


func startLeaderVote(rf *Raft) {
	args := RequestVoteArgs{}
	args.term = rf.currentTerm
	args.candidateId = rf.me
	args.lastLogIndex = rf.lastApplied
	args.lastLogTerm = rf.currentTerm

	reply := RequestVoteReply{}

	result := rf.sendRequestVote(rf.me, args, &reply)

	if (!result) {
		fmt.Printf("Error sending the message to the other servers")
	}
}


