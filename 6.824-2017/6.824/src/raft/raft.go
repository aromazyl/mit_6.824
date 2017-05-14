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

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Raft struct {
	mu              sync.Mutex
	peers           []*labrpc.ClientEnd
	persister       *Persister
	me              int
	currentTerm     int
	votedFor        int
	state           int
	timeout         int
	currentLeader   int
	lastMessageTime int64
	message         chan bool
	electCh         chan bool
	heartbeat       chan bool
	heartbeatRe     chan bool
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.currentTerm
	isLeader = rf.currentLeader == rf.me
	return term, isLeader

}

func (rf *Raft) setMessageTime(time int64) {
	rf.lastMessageTime = time
}

func (rf *Raft) setTerm(term int) {
	rf.currentTerm = term
}

func (rf *Raft) becomeCandidate() {
	rf.state = -1
	rf.setTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.currentLeader = -1
}

func (rf *Raft) becomeLeader() {
	rf.state = 2
	rf.currentLeader = rf.me
}

func (rf *Raft) becomeFollower(term int, candidate int) {
	rf.setTerm(term)
	rf.votedFor = candidate
	rf.setMessageTime(milliseconds())
}

func (rf *Raft) persist() {}

func (rf *Raft) readPersist(data []byte) {}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	return x + y - min(x, y)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogterm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	currentTerm, _ := rf.GetState()
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor != -1 && args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Lock()
		rf.setTerm(max(args.Term, currentTerm))
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		rf.becomeFollower(max(args.Term, currentTerm), args.CandidateId)
		rf.mu.Unlock()
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.mu.Lock()
		rf.currentLeader = args.LeaderId
		rf.votedFor = args.LeaderId
		rf.state = 0
		rf.setMessageTime(milliseconds())
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args AppendEntriesArgs, reply *AppendEntriesReply, timeout int) {
	c := make(chan bool, 1)
	go func() { c <- rf.peers[server].Call("Raft.AppendEntries", args, reply) }()
	select {
	case ok := <-c:
		if ok && reply.Success {
			rf.heartbeatRe <- true
		} else {
			rf.heartbeatRe <- false
		}
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		rf.heartbeatRe <- false
		break
	}
}

func randomRange(min, max int64) int64 {
	return rand.Int63n(max-min) + min
}

func milliseconds() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (rf *Raft) sendRequestVoteAndTrigger(server int, args RequestVoteArgs, reply *RequestVoteReply, timeout int) {
	c := make(chan bool, 1)
	go func() { c <- rf.sendRequestVote(server, args, reply) }()
	select {
	case ok := <-c:
		if ok && reply.VoteGranted {
			rf.electCh <- true
		} else {
			rf.electCh <- false
		}
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		rf.electCh <- false
		break
	}
}

func (rf *Raft) sendAppendEntriesImpl() {
	if rf.currentLeader == rf.me {
		var args AppendEntriesArgs
		var success_count int
		timeout := 20
		args.LeaderId = rf.me
		args.Term = rf.currentTerm
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				var reply AppendEntriesReply
				go rf.sendHeartBeat(i, args, &reply, timeout)
			}
		}
		for i := 0; i < len(rf.peers)-1; i++ {
			select {
			case ok := <-rf.heartbeatRe:
				if ok {
					success_count++
					if success_count >= len(rf.peers)/2 {
						rf.mu.Lock()
						rf.setMessageTime(milliseconds())
						rf.mu.Unlock()
					}
				}
			}
		}
		if success_count < len(rf.peers)/2 {
			rf.mu.Lock()
			rf.currentLeader = -1
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendLeaderHeartBeat() {
	timeout := 20
	for {
		select {
		case <-rf.heartbeat:
			rf.sendAppendEntriesImpl()
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			rf.sendAppendEntriesImpl()
		}
	}
}

func (rf *Raft) election_one_round() bool {
	var timeout int64
	var done int
	var triggerHeartbeat bool
	timeout = 100
	last := milliseconds()
	success := false
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()
	rpcTimeout := 20
	for {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				var args RequestVoteArgs
				server := i
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				var reply RequestVoteReply
				go rf.sendRequestVoteAndTrigger(server, args, &reply, rpcTimeout)
			}
		}
		done = 0
		triggerHeartbeat = false
		for i := 0; i < len(rf.peers)-1; i++ {
			select {
			case ok := <-rf.electCh:
				if ok {
					// need add ?
					done += 1
					success = done >= len(rf.peers)/2 || rf.currentLeader > -1
					success = success && rf.votedFor == rf.me
					if success && !triggerHeartbeat {
						triggerHeartbeat = true
						rf.mu.Lock()
						rf.becomeLeader()
						rf.mu.Unlock()
						rf.heartbeat <- true
					}
				}
			}
		}
		if (timeout+last < milliseconds()) || (done >= len(rf.peers)/2 || rf.currentLeader > -1) {
			break
		} else {
			select {
			case <-time.After(time.Duration(10) * time.Millisecond):
			}
		}
	}
	return success
}

func (rf *Raft) election() {
	var result bool
	for {
		timeout := randomRange(150, 300)
		rf.setMessageTime(milliseconds())
		for rf.lastMessageTime+timeout > milliseconds() {
			select {
			case <-time.After(time.Duration(timeout) * time.Millisecond):
				if rf.lastMessageTime+timeout <= milliseconds() {
					break
				} else {
					rf.setMessageTime(milliseconds())
					timeout = randomRange(150, 300)
					continue
				}
			}
		}
	}
	result = false
	for !result {
		result = rf.election_one_round()
	}
}

func (rf *Raft) Start(commnad interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	return index, term, isLeader
}

func (rf *Raft) Kill() {}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.setTerm(0)
	rf.votedFor = -1
	rf.state = 0
	rf.timeout = 0
	rf.currentLeader = -1
	rf.electCh = make(chan bool)
	rf.message = make(chan bool)
	rf.heartbeat = make(chan bool)
	rf.heartbeatRe = make(chan bool)
	rand.Seed(time.Now().UnixNano())

	go rf.election()
	go rf.sendLeaderHeartBeat()

	rf.readPersist(persister.ReadRaftState())
	return rf
}
