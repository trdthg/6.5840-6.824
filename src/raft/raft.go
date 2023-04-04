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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	RaftState
	votedFor    *int
	hartbeat    chan struct{}
	backStateCh chan struct{}

	// 2B
	// logs []string

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.getCurrentTerm(), rf.isLeader()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	ServerInfo
	VoteGranted bool
	Reason      string
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.ServerInfo = rf.getServerInfo()
	// Reply false if term < currentTerm
	if rf.getCurrentTerm() > args.Term {
		reply.VoteGranted = false
		reply.Reason = "server term is larger than candidate"
		return
	}
	if rf.getCurrentTerm() < args.Term {
		rf.setCurrentTerm(args.Term)
	}
	// If votedFor is null or candidateId,
	// TODO: and candidate’s log is at least as up-to-date as receiver’s log,
	// grant vote
	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		// TODO：是否有必要
		if rf.isLeader() || rf.isCandidate() {
			// rf.transToFollower()
			rf.backStateCh <- struct{}{}
		}
	} else {
		// 已经投过票
		reply.VoteGranted = false
		reply.Reason = "server has voted to other candidate"
	}
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	ServerInfo
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 接收到心跳
	reply.ServerInfo = rf.getServerInfo()
	DTrace("%s AppendEntries 收到心跳", rf.toString())
	if rf.isFollower() {
		// DTrace("%s 通知重置计时器", rf.toString())
		go func() {
			rf.hartbeat <- struct{}{}
		}()
	}
	if rf.getCurrentTerm() <= args.Term {
		// 候选者 / Leader 收到心跳后，如果自己的 term 较小，则退回为 follower
		// rf.currentTerm = args.Term
		if !rf.isFollower() {
			rf.backStateCh <- struct{}{}
			// select {
			// case rf.backStateCh <- struct{}{}:
			// default:
			// 	// do nothing
			// }
		}
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) transToLeader() {
	rf.resetVote()

	DNotice("%s become leader", rf.toString())
	rf.setRole(Leader)
	rf.startHeartbeat()
}

func (rf *Raft) transToFollower() {
	rf.resetVote()

	DNotice("%s become follower", rf.toString())
	rf.setRole(Follower)
	rf.handleHeartbeat()
}

func (rf *Raft) startHeartbeat() {
	// 发送心跳
	stopSendHeartBeatCh := make(chan struct{})
	go func() {
	out:
		for {
			select {
			case <-stopSendHeartBeatCh:
				DTmp("%s 旧的 Leader 退回 Follower, 停止发送心跳", rf.toString())
				break out
			default:
				// do nothing
			}
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				args := AppendEntriesArgs{
					Term:     rf.getCurrentTerm(),
					LeaderId: rf.me,
				}
				reply := AppendEntriesReply{}
				go func(i int) {
					if !rf.isLeader() {
						return
					}
					DTrace("%s send AppendEntries RPC to server [%d]", rf.toString(), i)
					ok := rf.sendAppendEntries(i, &args, &reply)
					// TODO: handle send heartbeat error
					if !ok {
						DError("%s failed to send AppendEntries RPC to server [%d]", rf.toString(), i)
					}
				}(i)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	DTrace("%s Leader 在等新 leader", rf.toString())
	<-rf.backStateCh
	stopSendHeartBeatCh <- struct{}{}
	// 收到来自 new leader 的 heartbeat，转换为 follower
	rf.transToFollower()
}

func (rf *Raft) startElection() {
	rf.resetVote()
	voteSum := 0

	// 给自己投一票
	voteSum += 1

	// acceptVoteDone := make(chan struct{})
	voteCh := make(chan struct{})

	// 发送投票请求
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:        rf.getCurrentTerm(),
			CandidateId: rf.me,
		}
		reply := RequestVoteReply{}
		go func(i int) {
			DInfo("%s send RequestVote RPC to server [%d]", rf.toString(), i)
			maxRetryCount := 3
			for retryCount := 0; retryCount < maxRetryCount; retryCount++ {
				// 失败重试
				ok := rf.sendRequestVote(i, &args, &reply)
				if !ok {
					DError("%s failed to send RequestVote RPC to server [%d]", rf.toString(), i)
					continue
				}
				break
			}
			if !reply.VoteGranted {
				DError("%s get reject from [%s] [%d] term [%d], reason: %s", rf.toString(), reply.Role.toString(), i, reply.Term, reply.Reason)
				return
			}
			select {
			case voteCh <- struct{}{}:
				DInfo("%s get vote from %s", rf.toString(), reply.ServerInfo.toString())
			// case <-acceptVoteDone:
			case <-time.After(10 * time.Millisecond):
				DInfo("%s doesn't accept vote anymore", rf.toString())
			}
		}(i)
	}

	waitToTransformToLeaderCh := make(chan struct{})
	DInfo("%s 原始地址 [%p]", rf.toString(), &waitToTransformToLeaderCh)
	acceptVoteDone := make(chan struct{}, 1)

	// 接受投票
	go func() {
		DInfo("%s 传入的地址1 [%p]", rf.toString(), &waitToTransformToLeaderCh)
	out:
		for {
			DInfo("%s 传入的地址2 [%p]", rf.toString(), &waitToTransformToLeaderCh)
			select {
			case <-voteCh:
				DInfo("%s 传入的地址3 [%p]", rf.toString(), &waitToTransformToLeaderCh)
				voteSum += 1
				DInfo("%s 传入的地址3.1 [%p]", rf.toString(), &waitToTransformToLeaderCh)

				// 如果已经收集到足够的投票，转换为 leader
				if voteSum > len(rf.peers)/2 {
					DInfo("%s >>> 票收齐嘞 [%p]", rf.toString(), &waitToTransformToLeaderCh)
					waitToTransformToLeaderCh <- struct{}{}
					DInfo("%s <<< 票收齐通知完嘞 [%p]", rf.toString(), &waitToTransformToLeaderCh)
					// acceptVoteDone <- struct{}{}
					DTmp("%s get enough vote, becoming leader", rf.toString())
					break out
				}
			case <-acceptVoteDone:
				break out
			}
		}
		DInfo("%s 传入的地址4 [%p]", rf.toString(), &waitToTransformToLeaderCh)
		DTmp("%s 不再接受投票", rf.toString())
	}()

	// 150 - 250 ms
	electionTimeout := time.Duration(rand.Intn(100)+110) * time.Millisecond
	waitVoteDoneTimer := time.NewTimer(electionTimeout)
	// 这里给出一部分时间，等待接收心跳，看看是否能发现现有的 leader
	DTmp("%s 等待超时或者 heartbeat", rf.toString())
	select {
	case <-waitVoteDoneTimer.C:
		DTmp("%s 不等嘞", rf.toString())
		switch rf.role {
		case Leader:
			panic("impossible")
		case Candidate:
			DTmp("%s 等到了吗, [%p]", rf.toString(), &waitToTransformToLeaderCh)
			// 如果没有转化为 follower，说明现在没有 leader
			select {
			case <-waitToTransformToLeaderCh:
				DTmp("%s wait vote timeout", rf.toString())
				// 收集到足够的投票，转换为 leader
				rf.transToLeader()
			case <-time.After(10 * time.Millisecond):
				DTmp("%s 选举平局", rf.toString())
				// 平局，不做处理，进入下一个 term，重新选举
			}
		case Follower:
			DError("%s election timeout, get vote sum: %d", rf.toString(), voteSum)
			// 如果超时，进入下一个 term
			// 这里不做处理，直接执行结束，自动进入下一个 term，保持 Candidate 身份，进行新一轮选举
		}
	case <-rf.backStateCh:
		// 转换为 follower, 进入下一个 term
		// 1. 收到来自 leader 的 heartbeat
		// 2. 接收到 Request Vote，自己的 term 比其他 candidate 低
		acceptVoteDone <- struct{}{}
		DTrace("%s 收到来自 leader 的 heartbeat，退回为 follower", rf.toString())
		rf.transToFollower()
	}
}

func (rf *Raft) handleHeartbeat() {
	for {
		DTrace("%s 启动超时定时器", rf.toString())
		// random int from 200-500
		randomTimeout := time.Duration(rand.Int63()%200+300) * time.Millisecond
		timer := time.NewTimer(randomTimeout)
		select {
		case <-timer.C:
			// 如果超时，开始选举
			DInfo("%s doFollowerShouldDo 超时，开始选举", rf.toString())
			// 在下一个 term 选举
			rf.setRole(Candidate)
			return
		case <-rf.hartbeat:
			// 如果收到心跳，重置定时器，继续等待
			DTrace("%s doFollowerShouldDo 收到心跳，重置超时定时器", rf.toString())
			timer.Stop()
		}
	}
}

func (rf *Raft) resetVote() {
	rf.votedFor = nil
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	DInfo("start ticker for server [%v]", rf.me)

	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		switch rf.role {
		case Leader:
			rf.startHeartbeat()
		case Candidate:
			// 开始选举 term 增加 1
			rf.addTerm()
			rf.startElection()
		case Follower:
			rf.handleHeartbeat()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.backStateCh = make(chan struct{}, 1)
	rf.hartbeat = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
