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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Command interface{}
	Term    int
}
type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

type VoteState int

const (
	Normal VoteState = iota // 正常投票
	Voted                   // 此任期内已经投过票
	Expire                  // 投票过期
	Killed                  // 当前raft节点已死亡
)

type AppendEntriesState int

const (
	AppendNormal    AppendEntriesState = iota // 正常追加
	AppendOutOfDate                           // 追加过时
	AppendKilled                              // Raft程序终止
	AppendRepeat                              // 重复追加
	AppendCommit                              // 追加的日志已提交
	MisMatch                                  // 追加不匹配
)

// 全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

type Timer struct {
	timer *time.Ticker
}

// 选举时间和称为领导后的心跳检测时间不一样的，reset是称为候选者后的选举超时时间，resetHeartBeat是称为领导者后的心跳超时时间
func (t *Timer) reset() {
	randomTime := time.Duration(150+rand.Intn(200)) * time.Millisecond
	t.timer.Reset(randomTime)
}
func (t *Timer) resetHeartBeat() {
	t.timer.Reset(HeartBeatTimeout)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int // 已知可以提交的日志条目索引
	lastApplied int // 已经提交了的日志条目索引
	nextIndex   []int
	matchIndex  []int

	// 当前Raft节点的相关状态
	state     Status //节点状态
	voteCount int    //节点获取的投票数量
	timer     Timer

	// 日志存储-3B使用
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := false
	if rf.state == Leader {
		isleader = true
	}
	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
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
	// Your code here (3C).
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
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	AppendState AppendEntriesState
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// raft节点crash
	if rf.killed() {
		reply.AppendState = AppendKilled
		reply.Term = -1
		reply.Success = false
		return
	}

	// 出现网络分区
	if args.Term < rf.currentTerm {
		reply.AppendState = AppendOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 对当前raft进行重置
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.voteCount = 0
	rf.state = Follower
	rf.timer.reset()

	// 对返回的reply进行赋值
	reply.AppendState = AppendNormal
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int //候选人的任期
	CandidateId  int //候选人ID
	LastLogIndex int //竞选人日志条目最后索引
	LastLogTerm  int //竞选人日志条目最后索引对应的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int       // 投票者raft节点的任期
	VoteGranted bool      // 是否投票标记，true代表投票，false代表不投票
	VoteState   VoteState // 投票状态,包含四种状态：正常投票、本任期Term内已经投过票、投票过期（网络分区、日志不是最新的）、raft节点死亡
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3A, 3B).

	// 当前节点crash
	if rf.killed() {
		reply.VoteState = Killed
		reply.VoteGranted = false
		reply.Term = -1
		return
	}

	// 出现网络分区
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 重置自身状态
	if args.Term > rf.currentTerm {
		rf.state = Follower
		// 符合投票条件，此时raft节点任期变为候选人任期
		rf.currentTerm = args.Term
		rf.voteCount = 0
		rf.votedFor = -1
	}

	// 没有投过票，或者出现上述情况，进行投票
	if rf.votedFor == -1 {
		//
		currentLogIndex := len(rf.log)
		currentLogTerm := 0
		if currentLogIndex > 0 {
			currentLogTerm = rf.log[currentLogIndex-1].Term
		}

		// 满足论文说明的第二个匹配条件，保证投票时日志是最新的
		// 1. 日志有不同的任期，此时有更大的任期的节点日志更新。
		// 2. 日志尾部的条目对应的任期相同，此时比较Index，下标更大的是最新的
		if args.LastLogTerm < currentLogTerm || (len(rf.log) > 0 && args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)) {
			reply.VoteState = Expire
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
		// 确保Candidate的LogEntry是最新的
		//if len(rf.log) > 0 && rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log) > args.LastLogIndex+1 {
		//	return
		//}
		//if len(rf.log) > 0 && rf.log[len(rf.log)-1].Term > args.LastLogTerm {
		//	return
		//}

		// 开始投票
		rf.votedFor = args.CandidateId
		rf.voteCount = 0
		reply.Term = rf.currentTerm
		reply.VoteState = Normal
		reply.VoteGranted = true //投票
		//fmt.Printf("[RequestVote] raft node : %v, voted for candidate : %v\n", rf.me, args.CandidateId)
		// 投完票后，本节点此时需要重置超时时间
		rf.timer.reset()
	} else { // 此任期内已经投过票了
		reply.VoteState = Voted
		reply.VoteGranted = false
		rf.voteCount = 0
		if rf.votedFor != args.CandidateId {
			// 当前节点是来自同一轮的，不同的竞选者,由于已经投过票了，此时直接返回
			return
		} else {
			//当前节点的票已经给了同一个人，但由于网络等原因，又发送了一次投票请求，此时再此重置本节点的状态
			rf.state = Follower
		}
		rf.timer.reset()
	}
	return
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
	if rf.killed() {
		return false
	}
	// RPC调用失败后继续重试直至成功
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	//fmt.Printf("[sendRequestVote] args is %v, reply is %v\n", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 出现网络分区，候选人的term比自己的还小，不予投票
	if args.Term < rf.currentTerm {
		return false
	}

	switch reply.VoteState {
	case Expire:
		{
			// 过期有两种情况 1.候选者的Term过期了，比投票人的还小；2. 节点的日志过期了，投票人的日志更新
			rf.state = Follower
			rf.voteCount = 0
			rf.timer.reset()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
		}
	case Normal, Voted:
		{
			// 如果每个raft节点在本任期内已经投过票了，那么此时返回的VoteGranted为false
			if reply.VoteGranted && reply.Term == rf.currentTerm && rf.voteCount < len(rf.peers)/2+1 {
				rf.voteCount++
			}
			if rf.voteCount >= len(rf.peers)/2+1 {
				//fmt.Printf("[sendRequestVote] who is leader %v, voteCount is %v\n", rf.me, rf.voteCount)
				rf.voteCount = 0
				// 如果此raft节点本身就是Leader，此时直接返回
				if rf.state == Leader {
					return ok
				}
				// 不是Leader，此时要初始化nextIndex数组
				rf.state = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.nextIndex); i++ {
					rf.nextIndex[i] = len(rf.log) + 1
				}
				rf.timer.resetHeartBeat()
			}
		}
	case Killed:
		{
			rf.voteCount = 0
			return false
		}

	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	// 这个锁，只能加在这里，如果加在rpc调用上面，对于Leader节点而言，某个raft节点crash或者网络原因无法取得联系
	// 此时上面的rpc调用一直重试，导致某个协程一直占有Leader节点的锁而不释放，导致其他心跳检测无法获取锁，一直阻塞。
	// 在出现网络故障时，出现上述情况，测试通不过。投票选举的锁道理类似
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对返回的reply进行状态判断
	switch reply.AppendState {
	case AppendOutOfDate:
		{
			// 出现网络分区，该Leader的Term已经比其他raft节点的term小，此时该Leader降为Follower,重置节点状态
			rf.state = Follower
			rf.votedFor = -1
			rf.voteCount = 0
			rf.currentTerm = reply.Term
			rf.timer.reset()
			return false
		}
	case AppendNormal:
		{
			return true
		}
	case AppendKilled:
		{
			return false
		}
	}
	return ok
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
	index := -1 // index为-1说明之前没有日志提交记录，只有出现日志提交记录时，index才不为-1
	term := -1
	isLeader := true

	// Your code here (3B).
	isLeader = rf.state == Leader
	if isLeader == false {
		return index, term, false
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timer.timer.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.timer.timer.C:
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			switch rf.state {
			case Follower: // 跟随者变为候选者
				rf.state = Candidate
				fallthrough // 继续执行下一个case分支，而不再重新检查条件
			case Candidate: // 成为候选者开始拉票
				// 初始化投票，把票投给自己
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount++
				// 每轮投票开始，重置选举过期时间
				rf.timer.reset()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					args := &RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log),
						LastLogTerm:  0,
					}
					if len(rf.log) > 0 {
						args.LastLogTerm = rf.log[len(rf.log)-1].Term
					}
					reply := &RequestVoteReply{}
					go rf.sendRequestVote(i, args, reply)
				}
			case Leader:
				// 成为领导者节点后开启心跳检测、日志同步
				rf.timer.resetHeartBeat()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					//if args.PrevLogIndex >= 0 {
					//	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					//}
					reply := &AppendEntriesReply{}
					go rf.sendAppendEntries(i, args, reply)
				}
			}
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = Follower
	rf.voteCount = 0
	rf.applyCh = applyCh
	// 初始随机时间为选举超时时间，使用rand.Intn()保证初始时间随机性
	rf.timer = Timer{timer: time.NewTicker(time.Duration(150+rand.Intn(200)) * time.Millisecond)}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
