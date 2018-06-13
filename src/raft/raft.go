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
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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

const (
	FOLLOWER   = iota
	CANDIDATER = iota
	LEADER     = iota
)

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
	term             int
	leader           int
	state            int
	voted            bool
	npeer            int
	timeout          time.Duration
	heartbeatTimeout time.Duration
	electionTimeout  time.Duration
	heartbeat        chan int

	index         int
	lastIndex     []int
	commitedIndex int
	logs          []LogEntry
	applyCh       chan ApplyMsg
	doPersist     chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = (rf.state == LEADER)
	return term, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.mu.Lock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.me)
	e.Encode(rf.term)
	e.Encode(rf.leader)
	e.Encode(rf.state)
	e.Encode(rf.voted)
	e.Encode(rf.npeer)
	e.Encode(rf.index)
	e.Encode(rf.lastIndex)
	e.Encode(rf.commitedIndex)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.mu.Unlock()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.me)
	d.Decode(&rf.term)
	d.Decode(&rf.leader)
	d.Decode(&rf.state)
	d.Decode(&rf.voted)
	d.Decode(&rf.npeer)
	d.Decode(&rf.index)
	d.Decode(&rf.lastIndex)
	d.Decode(&rf.commitedIndex)
	d.Decode(&rf.logs)
	rf.mu.Unlock()
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term          int
	Leader        int
	Index         int
	Entries       []LogEntry
	CommitedIndex int
}

type AppendEntriesReplys struct {
	Term          int
	LastIndex     int
	CommitedIndex int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term          int
	CommitedIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	IsAgree bool
	Term    int
	Index   int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//If me is behind term or me is not voted
	rf.mu.Lock()
	if args.CommitedIndex < rf.commitedIndex {
		reply.IsAgree = false
		reply.Index = rf.lastIndex[rf.me]
		reply.Term = rf.term
	} else if args.CommitedIndex > rf.commitedIndex {
		reply.IsAgree = true
		reply.Index = rf.lastIndex[rf.me]
		reply.Term = rf.term
	} else if args.Term > rf.term || (!rf.voted && args.Term == rf.term) {
		reply.IsAgree = true
		rf.voted = true
		rf.term = args.Term
		rf.state = FOLLOWER
		//rf.lastIndex[rf.me] = rf.commitedIndex
	} else {
		reply.IsAgree = false
	}
	rf.mu.Unlock()
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReplys) {
	rf.mu.Lock()
	//If HeartBeat has higher or equal term, candidate goes to follower
	if args.Term >= rf.term || args.CommitedIndex > rf.commitedIndex {
		if rf.state == CANDIDATER {
			rf.state = FOLLOWER
		}
		if args.Term > rf.term && rf.state == LEADER {
			rf.state = FOLLOWER
			//rf.lastIndex[rf.me] = rf.commitedIndex
			rf.leader = args.Leader
		}
		rf.leader = args.Leader
		rf.term = args.Term
		rf.heartbeat <- 0
	} else if args.CommitedIndex < rf.commitedIndex {
		reply.LastIndex = rf.lastIndex[rf.me]
		reply.Term = rf.term
		reply.CommitedIndex = rf.commitedIndex
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.term

	if len(args.Entries) > 0 {
		if rf.lastIndex[rf.me] < args.Entries[0].Index ||
			args.Entries[0] != rf.logs[args.Entries[0].Index] {
			reply.LastIndex = args.Entries[0].Index - 1
			reply.CommitedIndex = rf.commitedIndex
			rf.mu.Unlock()
			return
		} else {
			for i := 1; i < len(args.Entries); i++ {
				if len(rf.logs) > args.Entries[i].Index {
					rf.logs[args.Entries[i].Index] = args.Entries[i]
				} else if len(rf.logs) == args.Entries[i].Index {
					rf.logs = append(rf.logs, args.Entries[i])
				} else {
					fmt.Errorf("Log size to small\n")
				}
				rf.lastIndex[rf.me] = args.Entries[i].Index
			}
		}
	}
	if args.CommitedIndex > rf.commitedIndex && args.CommitedIndex <= rf.lastIndex[rf.me] {
		for i := rf.commitedIndex + 1; i <= args.CommitedIndex; i++ {
			var msg ApplyMsg
			msg.Command = rf.logs[i].Command
			msg.Index = rf.logs[i].Index
			rf.applyCh <- msg
		}
		rf.commitedIndex = args.CommitedIndex
	}
	reply.LastIndex = rf.lastIndex[rf.me]
	reply.CommitedIndex = rf.commitedIndex
	rf.mu.Unlock()
	rf.doPersist <- 0
}

func (rf *Raft) sendAppendEntrities(server int, args *AppendEntriesArgs, reply *AppendEntriesReplys) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	isLeader = rf.state == LEADER
	term = rf.term
	index = rf.lastIndex[rf.me] + 1

	if isLeader {
		rf.mu.Lock()
		rf.lastIndex[rf.me] += 1
		log := LogEntry{rf.lastIndex[rf.me], term, command}
		rf.logs = append(rf.logs, log)
		rf.mu.Unlock()
		//fmt.Printf("Commited %d,\t", rf.commitedIndex)
		//for i := 0; i < rf.npeer; i++ {
		//	fmt.Printf("%d\t", rf.lastIndex[i])
		//}
		//fmt.Println("")
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
	// Your code here, if desired.
}

func (rf *Raft) sendHeartBeats() {
	oks := make([]bool, rf.npeer)
	args := make([]AppendEntriesArgs, rf.npeer)
	replys := make([]AppendEntriesReplys, rf.npeer)
	//TODO stop send heartbeats when me is not leader
	for i := 0; i < rf.npeer; i++ {
		if i == rf.me {
			replys[i].Term = rf.term
			oks[i] = true
			continue
		} else {
			oks[i] = false

			//sendAppendEntrities
			go func(i int) {
				rf.mu.Lock()
				args[i].Leader = rf.me
				args[i].Term = rf.term
				args[i].Index = rf.lastIndex[rf.me]
				var commitedCount = 0
				for j := 0; j < rf.npeer; j++ {
					if rf.lastIndex[j] > rf.commitedIndex {
						commitedCount += 1
					}
				}
				if commitedCount*2 > rf.npeer {
					rf.commitedIndex += 1

					rf.doPersist <- 0
					//commit one log
					var msg ApplyMsg
					msg.Command = rf.logs[rf.commitedIndex].Command
					msg.Index = rf.logs[rf.commitedIndex].Index
					rf.applyCh <- msg
				}
				args[i].CommitedIndex = rf.commitedIndex
				if rf.lastIndex[rf.me] >= rf.lastIndex[i] {
					args[i].Entries = rf.logs[rf.lastIndex[i] : rf.lastIndex[rf.me]+1]
				}

				rf.mu.Unlock()

				oks[i] = rf.sendAppendEntrities(i, &args[i], &replys[i])
				rf.mu.Lock()
				if oks[i] {
					rf.lastIndex[i] = replys[i].LastIndex
					if replys[i].CommitedIndex > rf.commitedIndex {
						rf.state = FOLLOWER
					} else if replys[i].CommitedIndex == rf.commitedIndex &&
						replys[i].Term > rf.term {
						rf.state = FOLLOWER
					}
					if rf.state == FOLLOWER {
						//rf.lastIndex[rf.me] = rf.commitedIndex
						rf.doPersist <- 0
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}

	select {
	case <-time.After(rf.heartbeatTimeout):
		return
	}

	for i := 0; i < rf.npeer; i++ {
		if oks[i] && replys[i].Term > rf.term {
			rf.state = FOLLOWER
			rf.voted = false
			return
		}
	}
}

func (rf *Raft) enterLeaderElection() {
	rf.mu.Lock()
	if rf.voted {
		time.Sleep(time.Duration(300) * time.Millisecond)
		rf.mu.Unlock()
		return
	}
	rf.term += 1
	rf.state = CANDIDATER
	rf.voted = true
	rf.mu.Unlock()

	args := make([]RequestVoteArgs, rf.npeer)
	replys := make([]RequestVoteReply, rf.npeer)
	oks := make([]bool, rf.npeer)

	// WaitGroup can't be used here
	// Maybe there is some method can change state beforehead
	agree := make(chan int)
	agreeCount := int32(0)
	var agreeLock sync.Mutex

	checkAgree := func() {
		agreeLock.Lock()
		atomic.AddInt32(&agreeCount, 1)
		if agreeCount*2 > int32(rf.npeer) && (agreeCount-1)*2 <= int32(rf.npeer) {
			agree <- 0
		}
		agreeLock.Unlock()
	}

	go func() {
		for i := 0; i < rf.npeer; i++ {
			if i == rf.me {
				replys[i].IsAgree = true
				oks[i] = true
				checkAgree()
			} else {
				args[i].CommitedIndex = rf.commitedIndex
				args[i].Term = rf.term
				oks[i] = false
				go func(i int) {
					oks[i] = rf.sendRequestVote(i, &args[i], &replys[i])
					if oks[i] && replys[i].IsAgree {
						checkAgree()
					}
				}(i)
			}
		}
	}()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	electionTimeout := time.Duration(300+r.Int()%300) * time.Millisecond

	select {
	case <-agree:
		// me is Leader now
		rf.mu.Lock()
		rf.leader = rf.me
		rf.state = LEADER
		rf.voted = false
		for i := 0; i < rf.npeer; i++ {
			rf.lastIndex[i] = rf.lastIndex[rf.me]
		}
		rf.mu.Unlock()
	case <-time.After(electionTimeout):
		// not Leader, wait until next election
		rf.mu.Lock()
		rf.state = CANDIDATER
		rf.voted = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) reciveHeatBeats() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ms := rf.heartbeatTimeout*2 + time.Duration(r.Int()%100)*time.Millisecond
	select {
	case <-time.After(ms):
		rf.mu.Lock()
		rf.state = CANDIDATER
		rf.voted = false
		rf.mu.Unlock()
	case <-rf.heartbeat:
	}
}

func (rf *Raft) loop() {
	for {
		switch rf.state {
		case LEADER:
			rf.sendHeartBeats()
		case CANDIDATER:
			rf.enterLeaderElection()
		case FOLLOWER:
			rf.reciveHeatBeats()
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
	//Peer start with FOLLOWER state
	rf.npeer = len(rf.peers)
	rf.term = 0
	rf.state = FOLLOWER
	rf.voted = false
	rf.leader = -1
	rf.heartbeatTimeout = time.Duration(50) * time.Millisecond
	rf.heartbeat = make(chan int, 100)

	rf.index = 0
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{0, 0, 0})

	rf.lastIndex = make([]int, rf.npeer)
	for i := 0; i < rf.npeer; i++ {
		rf.lastIndex[i] = 0
	}

	rf.applyCh = applyCh

	rf.doPersist = make(chan int, 100)

	go func() {
		for {
			<-rf.doPersist
			rf.persist()
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//Peer try to LeaderElection
	go rf.loop()
	return rf
}
