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
import "time"
import "bytes"
import "rand"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type AppendEntryArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	leaderCommit int
}

type AppendEntryReply struct {
	term    int
	success bool
}

func (rf *Raft) resetLeaderTime() {
	rf.leaderTimer.Reset(10 * time.Second)
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.term < rf.currentTerm {
		reply.success = false
		reply.term = rf.currentTerm
		return
	}
	if len(rf.log)-1 < args.prevLogIndex ||
		rf.log[args.prevLogIndex].term < args.prevLogTerm {
		reply.success = false
		reply.term = rf.currentTerm
		return
	}
	rf.log = rf.log[:args.prevLogIndex+1]
	rf.log = append(rf.log, args.entries...)
	if args.leaderCommit > rf.commitIndex {
		rf.commitIndex = MaxInt(args.leaderCommit, len(rf.log)-1)
	}
	reply.success = true
	reply.term = rf.currentTerm
	return
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type LogEntry struct {
	Command interface{}
	term    int
}

type Raft struct {
	// 0 follower 1 candidate 2 server
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	mu                  sync.Mutex          // Lock to protect shared access to this peer's state
	peers               []*labrpc.ClientEnd // RPC end points of all peers
	persister           *Persister          // Object to hold this peer's persisted state
	me                  int                 // this peer's index into peers[]
	log                 []LogEntry
	currentTerm         int
	votedFor            int
	commitIndex         int
	lastApplied         int
	nextIndex           []int
	matchIndex          []int
	character           int
	granted_votes_count int
	state               string
	applyCh             chan ApplyMsg
	timer               *time.Timer
	leaderTimer         *time.Timer
	votedCount          int
	chanCommit          chan bool
	chanHeartBeat       chan bool
	chanLeader          chan bool
	chanVoteGranted     chan bool
	stateMachine        *StateMachine
}

func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		rf.timer = time.NewTimer(5 * time.Minute)
	}
	rand.Seed(42)
	rf.timer.Reset((randInt(150) + 150) * time.Second)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.character == 2 {
		isLeader = true
	} else {
		isLeader = false
	}
	return term, isLeader
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
	w := new(bytes.Buffer)
	enc := gob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	may_granted_vote := true
	if len(rf.log) > 0 {
		if rf.log[len(rf.log)-1].term > args.lastLogTerm {
			may_granted_vote = false
		}
		if rf.log[len(rf.log)-1].term == args.lastLogTerm &&
			len(rf.log) > args.lastLogIndex+1 {
			may_granted_vote = false
		}
	}
	if rf.currentTerm > args.term {
		reply.voteGranted = false
		reply.term = rf.currentTerm
		return
	}
	if rf.currentTerm == args.term {
		if (rf.votedFor == -1 || rf.votedFor == args.candidateId) && may_granted_vote {
			rf.votedFor = args.candidateId
			rf.persist()
		}
		reply.term = rf.currentTerm
		reply.voteGranted = (rf.votedFor == args.candidateId)
		return
	}
	rf.state = "FOLLOWER"
	rf.currentTerm = args.term
	rf.votedFor = -1
	if may_granted_vote {
		rf.votedFor = args.candidateId
		rf.persist()
	}
	rf.resetTimer()
	reply.term = args.term
	reply.voteGranted = (rf.votedFor == args.candidateId)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		term := rf.currentTerm
		if rf.state != "CANDIDATE" {
			return ok
		}
		if args.term != term {
			return ok
		}
		if reply.term != term {
			rf.currentTerm = reply.term
			rf.state = "FOLLOWER"
			rf.votedFor = -1
			rf.persist()
		}
		if reply.voteGranted {
			rf.votedCount++
			if rf.state == "CANDIDATE" && rf.votedCount > len(rf.peers)/2 {
				rf.state = "LEADER"
				rf.chanVoteGranted <- true
			}
		}
	}
	return ok
}

func (rf *Raft) BroadCastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args.term = rf.currentTerm
	args.candidateId = rf.me
	if len(rf.log) != 0 {
		args.lastLogIndex = len(rf.log) - 1
		args.lastLogTerm = rf.log[len(rf.log)-1].term
	} else {
		args.lastLogIndex = -1
		args.lastLogTerm = -1
	}

	chanB := make(chan int)
	for i := range rf.peers {
		if i != rf.me && rf.state == "CANDIDATE" {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, &args, &reply)
				chanB <- 1
			}(i)
		}
	}
	for i := range rf.peers {
		<-chanB
	}
	close(chanB)
}

func (rf *Raft) BroadCastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {

	}
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
	rf.stateMachine = &StateMachine{}
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = "FOLLOWER"
	rf.applyCh = applyCh
	rf.votedCount = 0
	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartBeat = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.resetTimer()

	go func(rf *Raft) {
		for {
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				// TODO snape shot
				applyMsg := ApplyMsg{rf.lastApplied, {}, true, make([]byte)}
				rf.stateMachine.Update(&applyMsg)
				go func() { rf.applyCh <- applyMsg }()
				continue
			}
			switch rf.state {
			case "FOLLOWER":
				{
					select {
					case <-chanHeartBeat:
						rf.resetTimer()
					case <-rf.time.C:
						{
							rf.mu.Lock()
							defer rf.mu.Unlock()
							rf.state = "CANDIDATE"
							go func() { rf.BroadCastRequestVote() }()
						}
					}
				}
			case "CANDIDATE":
				{
					select {
					case <-rf.time.C:
						rf.resetTimer()
						go func() { rf.BroadCastRequestVote() }()
					case <-rf.chanHeartBeat:
						rf.resetTimer()
						rf.state = "FOLLOWER"
					case <-rf.voteGranted:
						rf.state = "LEADER"
						rf.resetTimer()
					}
				}
			case "LEADER":
				{
					select {
					case <-rf.leaderTimer.C:
						rf.BroadCastAppendEntries()
						rf.resetLeaderTime()
					}
				}
			}
		}
	}(rf)

	return rf
}
