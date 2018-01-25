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

import "sync"
import "labrpc"
import "math/rand"
import "time"
import "fmt"

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
    FOLLOWER    = iota
    CANDIDATER  = iota
    LEADER      = iota
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
    term        int
    leader      int
    state       int
    voted       bool
    npeer       int
    timeout     time.Duration
    heartbeatTimeout time.Duration
    heartbeat    chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    term = rf.term
    isleader = (rf.state == LEADER)
    //fmt.Println("Peer %d, Term %d", rf.me, rf.term)
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
}


type AppendEntriesArgs struct {
    Term    int
    Leader  int
}

type AppendEntriesReplys struct {
    Term    int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term    int
    Index   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    IsAgree     bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    //If me is behind term or me is not voted
    if args.Term > rf.term || (!rf.voted && args.Term == rf.term) {
        reply.IsAgree = true
        rf.voted = true
        rf.term = args.Term
        rf.heartbeat <-0
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
    if args.Term >= rf.term {
        rf.leader = args.Leader
        rf.term = args.Term
        if rf.state == CANDIDATER {
            rf.state = FOLLOWER
        }
        rf.heartbeat <- 0
    }
    reply.Term = rf.term
    //fmt.Printf("Peer %d, My Term %d, leader Term %d.\n", rf.me, rf.term, reply.Term)
    rf.mu.Unlock()
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
    args := make([]AppendEntriesArgs, rf.npeer)
    replys := make([]AppendEntriesReplys, rf.npeer)
    //TODO stop send heartbeats when me is not leader
    for {
        //fmt.Printf("Leader %d send heartbeat\n", rf.me)
        var wg sync.WaitGroup
        wg.Add(rf.npeer)
        for i := 0; i < rf.npeer; i++ {
            if i == rf.me {
                replys[i].Term = rf.term
                wg.Done()
                continue
            } else {
                go func(i int) {
                    defer wg.Done()
                    args[i].Leader = rf.me
                    args[i].Term = rf.term
                    rf.sendAppendEntrities(i, &args[i], &replys[i])
                } (i)
            }
        }
        wg.Wait()
        for i := 0; i < rf.npeer; i++ {
            if replys[i].Term > rf.term {
                rf.state = FOLLOWER
                rf.voted = false
                go rf.enterLeaderElection()
                fmt.Printf("Peer %d is not leader any more.\n", rf.me)
                return
            }
        }
        time.Sleep(rf.heartbeatTimeout)
    }
}

func (rf *Raft) LeaderElection() bool {
    rf.mu.Lock()
    if rf.voted {
        rf.mu.Unlock()
        return false
    }
    rf.term += 1
    rf.state = CANDIDATER
    rf.voted = true
    rf.mu.Unlock()
    args := make([]RequestVoteArgs, rf.npeer)
    replys := make([]RequestVoteReply, rf.npeer)
    oks := make([]bool, rf.npeer)
    var wg sync.WaitGroup
    wg.Add(rf.npeer)
    for i := 0; i < rf.npeer; i++ {
        if i == rf.me {
            replys[i].IsAgree = true
            oks[i] = true
            wg.Done()
        } else {
            args[i].Index = i
            args[i].Term = rf.term
            go func(i int) {
                defer wg.Done()
                oks[i] = rf.sendRequestVote(i, &args[i], &replys[i])
            } (i)
        }
    }
    wg.Wait()
    count := 0
    for i := 0; i < rf.npeer; i++ {
        if oks[i] && replys[i].IsAgree {
            count += 1
        }
    }
    if rf.npeer < 2 * count {
        rf.mu.Lock()
        rf.leader = rf.me
        rf.state = LEADER
        rf.mu.Unlock()
        fmt.Printf("Peer %d is now leader.\n", rf.me)
        return true
    }
    return false
}

func (rf *Raft) enterLeaderElection() {
   r := rand.New(rand.NewSource(time.Now().UnixNano()))
   var ms time.Duration
   for {
       //ok means rf has be elected as leader
       ok := rf.LeaderElection()
       if ok {
           go rf.sendHeartBeats()
           return
       }
       //Leader election finish, now wait for heartbeat or timeout
       ms = time.Duration(r.Intn(100) + 300) * time.Millisecond
       select {
       case <-rf.heartbeat:
           ms = time.Duration(r.Intn(100) + 300) * time.Millisecond
           continue
       case <-time.After(ms):
           rf.mu.Lock()
           rf.voted = false
           rf.mu.Unlock()
           continue
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
    fmt.Printf("%d peers.\n", len(peers))
    rf.npeer = len(rf.peers)
    rf.term = 0
    rf.state = FOLLOWER
    rf.voted = false
    rf.leader = -1
    rf.heartbeatTimeout = time.Duration(50) * time.Millisecond
    rf.heartbeat = make(chan int, 10)

    //Peer try to LeaderElection
    go rf.enterLeaderElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
