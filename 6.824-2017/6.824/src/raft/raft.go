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
import "math/rand"
import "encoding/gob"
import "log"
import "fmt"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) resetLeaderTime() {
	if rf.leaderTimer == nil {
		log.Printf("raft leader timer inital...")
		rf.leaderTimer = time.NewTimer(time.Duration(10) * time.Millisecond)
	}
	rf.leaderTimer.Reset(time.Duration(10) * time.Millisecond)
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if len(rf.log)-1 < args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].term < args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.mu.Lock()
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = MaxInt(args.LeaderCommit, len(rf.log)-1)
	}
	rf.mu.Unlock()
	reply.Success = true
	reply.Term = rf.currentTerm
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
		rf.timer = time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	}
	rand.Seed(42)
	rf.timer.Reset(time.Duration(rand.Intn(150)+150) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == "LEADER" {
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (args *RequestVoteArgs) Dump() string {
	return fmt.Sprintf("args.term=[%v], args.candidateId=[%v], args.lastLogIndex=[%v], lastLogTerm=[%v]",
		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

func (reply *RequestVoteReply) Dump() string {
	return fmt.Sprintf("reply.term=[%v], reply.voteGranted=[%v]",
		reply.Term, reply.VoteGranted)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	may_granted_vote := true
	if len(rf.log) > 0 {
		if rf.log[len(rf.log)-1].term > args.LastLogTerm {
			may_granted_vote = false
		}
		if rf.log[len(rf.log)-1].term == args.LastLogTerm &&
			len(rf.log) > args.LastLogIndex+1 {
			may_granted_vote = false
		}
	}
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm == args.Term {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && may_granted_vote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}
	rf.state = "FOLLOWER"
	rf.currentTerm = args.Term
	rf.votedFor = -1
	if may_granted_vote {
		rf.votedFor = args.CandidateId
		rf.persist()
	}
	reply.Term = args.Term
	reply.VoteGranted = (rf.votedFor == args.CandidateId)
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
func (rf *Raft) sendRequestVote(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	if len(rf.log) != 0 {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[len(rf.log)-1].term
	} else {
		args.LastLogIndex = -1
		args.LastLogTerm = -1
	}
	var reply RequestVoteReply
	log.Printf("in send request vote")
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if ok {
		term := rf.currentTerm
		if rf.state != "CANDIDATE" {
			return ok
		}
		if args.Term != term {
			return ok
		}
		if reply.Term != term {
			rf.currentTerm = reply.Term
			rf.state = "FOLLOWER"
			rf.votedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.votedCount++
			log.Printf("raft votedCount:%d", rf.votedCount)
			if rf.state == "CANDIDATE" && rf.votedCount > len(rf.peers)/2 {
				// rf.state = "LEADER"
				rf.chanVoteGranted <- true
			}
		}
	}
	return ok
}

func (rf *Raft) BroadCastRequestVote() {
	log.Printf("raft:%v, broad cast request vote, peers number:%v", rf.me, len(rf.peers))
	chanB := make(chan int)
	for i := range rf.peers {
		if i != rf.me && rf.state == "CANDIDATE" {
			go func(i int) {
				rf.sendRequestVote(i)
				chanB <- 1
			}(i)
		}
	}
	for i := range rf.peers {
		i = i
		<-chanB
	}
	close(chanB)
}

func (rf *Raft) BroadCastAppendEntries() {
	// TODO
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		num := 0
		for j := range rf.peers {
			if rf.matchIndex[j] >= i && rf.log[i].term == rf.currentTerm {
				num++
			}
		}
		if num*2 > len(rf.peers) {
			rf.mu.Lock()
			rf.commitIndex = i
			rf.mu.Unlock()
		}
	}

	cntChan := make(chan int)
	for i := range rf.peers {
		if len(rf.log)-1 >= rf.nextIndex[i] {
			go func(rf *Raft, i int) {
				args := AppendEntryArgs{rf.currentTerm,
					rf.me, rf.nextIndex[i], rf.log[rf.nextIndex[i]].term, nil, rf.commitIndex}
				reply := AppendEntryReply{}
				args.Entries = make([]LogEntry, 0)
				if len(rf.log)-1 > rf.nextIndex[i] {
					for j := rf.nextIndex[i]; j < len(rf.log)-1; j++ {
						args.Entries = append(args.Entries, rf.log[j])
					}
				}
				tag := false
				ok := false
				for !tag {
					for !ok {
						ok = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
						if !ok {
							log.Printf("raft append entries call failure, peers[%v]", i)
						} else {
							log.Printf("raft append entries call success, peers[%v]", i)
						}
					}
					if len(rf.log)-1 > rf.nextIndex[i] {
						if !reply.Success {
							rf.mu.Lock()
							rf.nextIndex[i]--
							rf.mu.Unlock()
							args.Entries = append([]LogEntry{rf.log[rf.nextIndex[i]]}, args.Entries...)
						} else {
							rf.mu.Lock()
							rf.nextIndex[i] = len(rf.log) - 1
							rf.mu.Unlock()
							tag = true
						}
					}
				}
				cntChan <- 0
			}(rf, i)
		}
	}
	for i := range rf.peers {
		i = i
		<-cntChan
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
	rf.chanVoteGranted = make(chan bool, 100)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.resetTimer()

	go func(rf *Raft) {
		for {
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				// TODO snape shot
				applyMsg := ApplyMsg{rf.lastApplied, nil, true, make([]byte, 0)}
				// rf.stateMachine.Update(&applyMsg)
				go func() { rf.applyCh <- applyMsg }()
				continue
			}
			log.Printf("state:%v", rf.state)
			switch rf.state {
			case "FOLLOWER":
				{
					select {
					case <-rf.chanHeartBeat:
						log.Printf("FOLLOWER: chanHeartBeat received, rf id:%v", rf.me)
						rf.resetTimer()
					case <-rf.timer.C:
						{
							log.Printf("FOLLOWER: rf.time.C, rf id:%v", rf.me)
							rf.state = "CANDIDATE"
							rf.currentTerm++
							go func() { rf.BroadCastRequestVote() }()
							rf.resetTimer()
						}
					}
				}
			case "CANDIDATE":
				{
					select {
					case <-rf.timer.C:
						log.Printf("CANDIDATE: rf.time.C, rf id:%v", rf.me)
						go func() { rf.BroadCastRequestVote() }()
					case <-rf.chanHeartBeat:
						log.Printf("CANDIDATE: heartBeat, rf id:%v", rf.me)
						rf.resetTimer()
						rf.state = "FOLLOWER"
					case <-rf.chanVoteGranted:
						log.Printf("CANDIDATE: voteGranted, rf id:%v", rf.me)
						rf.state = "LEADER"
						rf.resetTimer()
					}
				}
			case "LEADER":
				{
					log.Printf("LEADER: broad casting entries, rf:%v id:%v", rf, rf.me)
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
