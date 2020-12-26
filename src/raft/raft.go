package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//FLLOWER
const (
	FLLOWER   = 1
	CANDIDATE = 2
	LEADER    = 3
	//身份
	/*raft0 = "FOLLOWER"
	raft1 = "CANDIDATE"
	raft2 = "LEADER"*/
	electionTimeout  = 150 // ms
	heartbeatTimeout = 50  // ms
)

// ApplyMsg struct
// as each Raft peer becomes aware that Successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3

	//CommandValid bool
	//Command      interface{}
	//CommandIndex int
}

// LogEntry structure
type LogEntry struct {
	Term    int
	Command interface{} ///todo
}

// Raft struct
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd //server编号，从0开始
	persister *Persister
	me        int // index into peers[]

	// Your data here.

	//Persistent state on all servers(Updated on stable storage before responding to RPCs)
	currentTerm int
	//latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor int        //candidateId that received vote in current Term (or null if none)//当前任期投票给了谁，没投则是-1
	log      []LogEntry //log Entries

	//Volatile state on all servers
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders(Reinitialized after election)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically

	//添加的变量
	state     int //身份
	voteCount int //获得的票数

	//两个timeout计时器
	electionTimeout  *time.Timer //follower和candidate需要使用的
	heartbeatTimeout *time.Timer //leader需要使用的

	//提交
	applyCh chan ApplyMsg

	//成功投票
	//voteCh chan struct{}

	m1 map[int]string /*{
		0:"FOLLOWER",
		1:"CANDIDATE",
		3:"LEADER"
	}*/
	isKilled bool

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//log.Print("raft->GetState")

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock() ///todo 这里加锁会造成死锁？
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//log.Print("raft->persist")
	// Your code here.
	// Example:
	//rf.mu.Lock() ///todo 这里加锁会造成死锁？
	//defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	//log.Print("raft->readPersist")
	// Example:///todo
	/*r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)*/

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// error...
		panic("fail to decode state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// RequestVoteArgs struct
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int //candidate’s Term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //Term of candidate’s last log entry

	//from int //谁发出的
}

// RequestVoteReply struct
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
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
	//log.Println(">>>>>>>>>>", rf.m1[rf.state], rf.me, "Term", rf.currentTerm, "正式向sever", server, "sendRequestVote >>>>>>>>>>")
	////log.Println(rf.m1[rf.state], rf.me, "args Term", args.Term, "正式sendRequestVote to sever", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

// RequestVote 其它server对于sendRequestVote的处理
// example RequestVote RPC handlr.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	//log.Println("<<< server", rf.me, "开始处理RequestVote...")
	// Your code here.

	rf.mu.Lock() ///todo
	defer rf.mu.Unlock()
	//不管是否vote，都要把reply的term设置为rf的term
	reply.Term = rf.currentTerm

	lastLogIndex := rf.lastLogIndex()
	//如果args的不是最新的
	if (args.LastLogTerm < rf.log[lastLogIndex].Term) ||
		(args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex < lastLogIndex) {
		//log.Println("<<< server", rf.me, "term", rf.currentTerm, "不投票")
		reply.VoteGranted = false
		/*******可以删除***************/
		/*reason := ""
		if rf.currentTerm > args.Term {
			reason = "RPC term is outdated"
		} else if args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex < lastLogIndex {
			reason = "Candidate's log is outdated"
		}*/
		//[12/17] log.Printf("refuse to vote for Node %d(Term %d), %s", args.CandidateId, args.Term, reason)
		/**********************************/
	} else { //如果args是最新的//如果Term比rpc的小，则更新Term并切换成follower身份
		//更新rf的term
		rf.currentTerm = args.Term
		//向args投票
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		//如果不是follower则退回follower
		//log.Println("<<< server", rf.me, "term", rf.currentTerm, "投票")
		if rf.state != FLLOWER {
			//log.Println("<<< server", rf.me, "退回FOLLOWER")
			rf.changeStateTo(FLLOWER)
		}
		//[12/17] log.Printf("accept vote for Node %d(Term %d), %v",
		//	args.CandidateId, args.Term, []int{args.LastLogTerm, args.LastLogIndex, rf.log[lastLogIndex].Term, lastLogIndex})

		//是不是要持久化
		rf.persist()
	}

}

// AppendEntriesArgs struct
// example AppendEntriesArgs RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int        //leader’s Term
	LeaderID     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //Term of PrevLogIndex entry
	Entries      []LogEntry //log Entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}

// AppendEntriesReply struct
// example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term          int  //currentTerm, for leader to update itself
	Success       bool //true if follower contained entry matching PrevLogIndex and PrevLogTerm
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//log.Println(">>>>>>>>>>", rf.m1[rf.state], rf.me, "Term", rf.currentTerm, "向sever", server, "sendAppendEntries >>>>>>>>>>")
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

// AppendEntries 其它服务器对于sendAppendEntries的处理
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Print("raft->AppendEntriesArgs to sever:" + strconv.Itoa(server))
	//rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.LeaderID == rf.me { //不需要给自己sendAppendEntries
		reply.Success = false
		return
	}

	if rf.currentTerm > args.Term { //如果LEADER发来的term反而比rf的小，则直接返回false
		// //[12/17] log.Printf("Node %d(Term %d) ignored append from leader %d(Term %d) after entry {%d, %d}",
		//   rf.me, rf.currentTerm, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false //可能会导致concurrent过不了
		return
	}

	rf.currentTerm = reply.Term

	//如果身份不是FOLLOWER则切换为FOLLOWER
	if rf.state != FLLOWER {
		rf.changeStateTo(FLLOWER)
	}

	/*****************/
	if rf.lastLogIndex() < args.PrevLogIndex || // rf.log的长度比prevLogIndex小
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // rf.log的term和PrevLogTerm不匹配
		//[12/17] log.Printf("refuse to append from leader %d(Term %d) after entry {%d, %d}, log not match",
		//	args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false //失败
		conflictIndex := -1
		//conflictTerm := -1
		if rf.lastLogIndex() >= args.PrevLogIndex { // rf.log的长度比prevLogIndex大或相等
			//conflictTerm := rf.log[args.PrevLogIndex].Term
			/*index := args.PrevLogIndex
			for ; index > 0; index-- {
				if rf.log[index].Term != rf.log[args.PrevLogIndex].Term {
					break
				}
			}*/
			//reply.ConflictIndex = index + 1
			conflictIndex = args.PrevLogIndex //- 1 + 1
		} else { //if rf.lastLogIndex() < args.PrevLogIndex{// rf.log的长度比prevLogIndex小
			//reply.ConflictIndex = rf.lastLogIndex() + 1
			conflictIndex = rf.lastLogIndex() + 1 //下一次从log长度+1的index复制日志条目
		}
		reply.ConflictIndex = conflictIndex
		//reply.ConflictTerm = conflictTerm
	} else {
		reply.Success = true //成功

		if len(args.Entries) > 0 {
			//如果心跳中携带了日志条目，并且match上了
			//则更新rf的log
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...) //从LEADER的nextIndex开始复制
			//将心跳携带的日志条目添加到rf的log里

			//持久化
			rf.persist()
		} else {
			//没有携带日志条目，是一个空的心跳
			//
		}

		// 更新 commitIndex
		if rf.commitIndex < args.LeaderCommit {
			if args.LeaderCommit <= rf.lastLogIndex() { //这里本来没有等于号//如果LEADERcommit的index没超过rf的logIndex
				//如果leader的commitIndex不超过rf的log的index
				//则将rf的commitIndex更新为leader的commitIndex
				rf.commitIndex = args.LeaderCommit
			} else {
				//如果超了
				//则更新为自己log的最后一个index
				rf.commitIndex = rf.lastLogIndex()
			}
		} /*else if rf.commitIndex == args.LeaderCommit { //这种情况好像也不需要考虑
			if args.LeaderCommit <= rf.lastLogIndex() {
				//如果leader的commitIndex不超过rf的log的index
				//则将rf的commitIndex更新为leader的commitIndex
				rf.commitIndex = args.LeaderCommit
			} else {
				//如果超了
				//则更新为自己log的最后一个index
				rf.commitIndex = rf.lastLogIndex()
			}
		} else { //rf.commitIndex > args.LeaderCommit
			//应该不存在这种情况
		}*/

		if len(args.Entries) > 0 {
			//[12/17] log.Printf("accept append %d entries from leader %d(Term %d) from entry {%d, %d}; commited = %d, leaderCommit = %d, logLen = %d, lastTerm = %d",
			//	len(args.Entries), args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm,
			//	rf.commitIndex, args.LeaderCommit, len(rf.log), rf.log[len(rf.log)-1].Term)
		}

	}
	/******************/
}

// Start ：
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//log.Print("raft->Start")

	index := -1
	term := -1
	isLeader := false
	if rf.state == LEADER {
		index = len(rf.log)
		term = rf.currentTerm
		isLeader = true
		//newEntry := LogEntry{rf.currentTerm, command}
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		rf.matchIndex[rf.me] = len(rf.log)
		//持久化
		rf.persist() ///todo
	}

	return index, term, isLeader
}

// Kill ：
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	//log.Print("raft->Kill")
	// Your code here, if desired.
	rf.isKilled = true
	//[12/17] log.Printf("is killed")
}

// Make ：
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
	//log.Print("raft->Make")
	/*
		// 再使用make函数创建一个非nil的map，nil map不能赋值
		m1 := make(map[int]string)
		// 最后给已声明的map赋值
		m1[0] = "FOLLOWER"
		m1[1] = "CANDIDATE"
		m1[2] = "LEADER"
	*/

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	// 异步执行循环
	//go rf.stateMachine()
	/**************************************************************/

	//Persistent state on all servers
	rf.currentTerm = 0           //初始Term为0
	rf.votedFor = -1             //初始没有投票
	rf.log = make([]LogEntry, 1) //0位置放置一个空条目，后续的条目index从1开始

	//Volatile state on all servers
	rf.commitIndex = 0 //初始committed为0
	rf.lastApplied = 0 //初始entry applied为0*/

	//Volatile state on leaders
	rf.nextIndex = make([]int, len(rf.peers))  //leader要发给每一个server的下一个log的index
	rf.matchIndex = make([]int, len(rf.peers)) //每一个server已知的最高的已经replicated的log的index
	for server := range rf.nextIndex {
		//初始nextIndex为leader最后一个log的index+1
		rf.nextIndex[server] = rf.lastLogIndex() + 1
	}
	for server := range rf.nextIndex {
		//初始matchIndex为0
		rf.matchIndex[server] = 0
	}

	//初始身份都是follower
	rf.state = FLLOWER

	//应用到状态机的通道
	rf.applyCh = applyCh
	/*
		rand.Seed(time.Now().UnixNano())                                                         //设置随机种子
		rf.electionTimeout = time.NewTimer(time.Duration(rand.Intn(151)+150) * time.Millisecond) //150ms到300ms之间的随机值
		rf.heartbeatTimeout = time.NewTimer(100 * time.Millisecond)                              //heartbeat的timeout固定为100ms

		rf.applyCh = applyCh ///todo ?
	*/

	// initialize from state persisted before a crash
	//rf.mu.Lock() ///todo
	rf.readPersist(persister.ReadRaftState()) ///?
	//rf.mu.Unlock()
	//[12/17] log.Printf("Initialize")

	// 选举线程（FOLLOWER和CANDIDATE）
	go rf.electThread()

	// 心跳线程（LEADER）
	go rf.heartbeatThread()

	// 应用线程
	go rf.applyThread()

	//状态机写法
	//go rf.stateMachine()

	return rf ///
}

// 选举线程（用go调用）
func (rf *Raft) electThread() {

	//var counterLock sync.Mutex ///删掉之后backup有可能过不了
	for {
		//终止
		if rf.isKilled {
			return
		}
		//threadSleep(getDuration())//todo 计时器放在这里会出问题

		rf.mu.Lock()
		if rf.state == FLLOWER { // ONLY follower would have election timeout
			//rf.state = CANDIDATE
			rf.changeStateTo(CANDIDATE)
			//rf.currentTerm++//变成candidate之后不能立刻修改term，不然会出错
		} else if rf.state == LEADER {
			rf.mu.Unlock()
			continue
		} else if rf.state == CANDIDATE {
			//不做处理
		}
		rf.mu.Unlock()

		//CANDIDATE等待一段时间之后发起选举
		//duration := time.Duration(electionTimeout + rand.Intn(electionRandomFactor*2) - electionRandomFactor)
		//Random(-electionRandomFactor, electionRandomFactor))
		//time.Sleep(rf.getDuration() * time.Millisecond)
		threadSleep(getDuration())

		//发起选举
		rf.mu.Lock() //调用ellect需要加锁
		if rf.state == CANDIDATE {
			//是CANDIDATE则发起选举
			rf.elect()
		} else {
			//不是CANDIDATE
		}
		rf.mu.Unlock()
	}
}

// 心跳线程（用go调用）
func (rf *Raft) heartbeatThread() {
	for {
		//终止
		if rf.isKilled {
			return
		}

		//等待一段时间之后发送心跳
		//time.Sleep(heartbeatTimeout * time.Millisecond)
		threadSleep(heartbeatTimeout)

		//发送心跳
		rf.mu.Lock() //调用heartbeat需要加锁
		if rf.state == LEADER {
			rf.heartbeat()
		} else {
			//不是LEADER
		}
		rf.mu.Unlock()
	}
}

// 应用线程（用go调用）
func (rf *Raft) applyThread() {
	// All servers
	//go func() {
	for {
		if rf.isKilled {
			return
		}

		rf.mu.Lock()
		rf.apply(rf.applyCh)
		rf.mu.Unlock()

		//time.Sleep(50 * time.Millisecond)
		threadSleep(50)
	}
	//}()
}

// 线程睡眠（单位：毫秒）
func threadSleep(sleepTime time.Duration) {
	time.Sleep(sleepTime * time.Millisecond)
}

// 选举函数（调用需要加锁）
func (rf *Raft) elect() {
	//log.Println(rf.m1[rf.state], rf.me, "开始选举...")
	//rf.mu.Lock()       ///TODO 这个锁应该加在哪里？
	//rf.mu.Unlock()
	//[12/17] log.Printf("start to request votes for term %d", rf.currentTerm+1)
	//选举开始时将rf得到的选票置位0
	//rf.votedFor = rf.me //投票给自己//下面的for循环已经包括了自己
	rf.voteCount = 0 ///TODO 自己的票算不算？//算，因为下面的循环也会向自己请求投票

	var requestVoteArgs RequestVoteArgs
	requestVoteArgs.Term = rf.currentTerm + 1
	requestVoteArgs.CandidateId = rf.me
	lastLogIndex := rf.lastLogIndex()
	requestVoteArgs.LastLogTerm = rf.log[lastLogIndex].Term
	requestVoteArgs.LastLogIndex = lastLogIndex

	for server := range rf.peers {
		go func(server int) {

			// 身份突然不是CANDIDATE则退出选举
			rf.mu.Lock()
			if rf.state != CANDIDATE {
				//[12/17] log.Printf("身份突然不是CANDIDATE了，退出选举")
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			var requestVoteReply RequestVoteReply
			//向sever发送请求投票的rpc
			ok := rf.sendRequestVote(server, requestVoteArgs, &requestVoteReply)

			/*rf.mu.Lock()
			// 身份突然不是CANDIDATE则退出选举
			if rf.state != CANDIDATE {//这里加上也不对
				//[12/26] log.Printf("身份突然不是CANDIDATE了，退出选举")
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()*/

			//判断Call"Raft.RequestVote"是否成功
			if ok { //Call"Raft.RequestVote"成功
				//加锁
				rf.mu.Lock()
				/*if rf.state != CANDIDATE {//这里加上为什么会全错
					//[12/26] log.Printf("身份突然不是CANDIDATE了，退出选举")
					rf.mu.Unlock()
					return
				}*/
				//如果回复的term更大，则更新自己的term并退回follower
				if requestVoteReply.Term > rf.currentTerm { //todo 这里不确定是否要+1
					rf.changeStateTo(FLLOWER)
					rf.currentTerm = requestVoteReply.Term
					//持久化
					rf.persist()
				} else { //if requestVoteArgs.Term == rf.currentTerm {//todo去掉这个可能会导致concurrent过不去
					if requestVoteReply.VoteGranted {
						//如果回复的term相等并且同意投票
						//投票计数器增加一票
						rf.voteCount++
						if rf.isMajority(rf.voteCount) && rf.state != LEADER { //如果得到多数投票且不是LEADER//todo这里加上!=LEADER可能导致backup的test过不去但是可能会让fugure8通过
							//rf.state = LEADER
							rf.changeStateTo(LEADER)
							//当选后任期才增加
							rf.currentTerm = requestVoteArgs.Term //这里很关键，不能用自己的任期自增
							//rf.currentTerm++
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							// immediately send heartbeats to others to stop election
							for i := range rf.peers {
								//rf.nextIndex设置为leader的最后一个entry的index+1
								rf.nextIndex[i] = rf.lastLogIndex() + 1
							}
							//持久化
							rf.persist()

							//[12/17] log.Printf("become leader for term %d, nextIndex = %v, requestVoteArgs = %v", rf.currentTerm, rf.nextIndex, requestVoteArgs)
						}
					} else {
						//拒绝投票
					}
				} /*else {
					//requestVoteArgs.Term < rf.currentTerm
					//[12/17] log.Printf("requestVoteArgs.Term < rf.currentTerm")
				}*/
				rf.mu.Unlock()
			} else { //Call"Raft.RequestVote" 失败
				////[12/17] log.Printf("Call"Raft.RequestVote"失败")
			}

		}(server)
	}
}

// 心跳函数（调用需要加锁）
func (rf *Raft) heartbeat() {

	//log.Println(rf.m1[rf.state], rf.me, "开始发送心跳...")
	////log.Println(rf.m1[rf.state], rf.me, "rf.nextIndex", rf.nextIndex)
	//rf.mu.Lock()
	//me := rf.me
	//defer rf.mu.Unlock()
	for server := range rf.peers {
		go func(server int) {
			for { //for循环不断重复发送，直到success才break
				//LEADER不需要给自己发送心跳
				if server == rf.me {
					break
				}

				// 如果身份不是LEADER了就退出
				if rf.state != LEADER {
					//[12/17] log.Printf("身份突然不是LEADER了")
					return //这里return和break作用好像是相同的
				}

				rf.mu.Lock()
				var appendEntriesArgs AppendEntriesArgs
				appendEntriesArgs.Term = rf.currentTerm
				appendEntriesArgs.LeaderID = rf.me
				appendEntriesArgs.PrevLogIndex = rf.nextIndex[server] - 1
				/*if appendEntriesArgs.PrevLogIndex < 0 { //todo 这里有可能越界，还没找到原因
					appendEntriesArgs.PrevLogIndex = 0
				}*/
				appendEntriesArgs.PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex].Term
				appendEntriesArgs.Entries = rf.log[rf.nextIndex[server]:]
				appendEntriesArgs.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()

				var appendEntriesReply AppendEntriesReply
				//向sever发送心跳rpc
				ok := rf.sendAppendEntries(server, appendEntriesArgs, &appendEntriesReply)

				//判断Call"Raft.AppendEntries"是否成功
				rf.mu.Lock()
				if ok { //Call"Raft.AppendEntries" 成功
					//if rf.currentTerm == appendEntriesArgs.Term { // 好像不需要ensure the reply is not outdated
					if appendEntriesReply.Term > rf.currentTerm { // LEADER过时
						rf.currentTerm = appendEntriesReply.Term
						//rf.state = FLLOWER
						rf.changeStateTo(FLLOWER)
						//持久化
						rf.persist()
						rf.mu.Unlock()
						return //LEADER过时后直接退出，不再继续发送心跳
					} else { //LEADER没有过时
						if appendEntriesReply.Success { //match成功，则更新rf里对于该server的matchIndex和nextIndex
							rf.matchIndex[server] = rf.lastLogIndex()
							//如果match成功则把nextIndex设置为自己log的index+1
							rf.nextIndex[server] = rf.lastLogIndex() + 1

							rf.mu.Unlock()
							break //success后跳出循环，不再继续发送心跳
						} else { // 没有match上，则修正rf.nextIndex
							rf.nextIndex[server] = appendEntriesReply.ConflictIndex
							//没success则继续循环，重新发送心跳
						}
					}

				} else {
					//Call"Raft.AppendEntries" 失败
				}
				rf.mu.Unlock()
			}
		}(server)
	}
	//更新LEADER的commitIndex
	rf.updateCommitIndex()
}

// 应用到状态机
func (rf *Raft) apply(applyCh chan ApplyMsg) {
	//for index:= rf.lastApplied;index<=rf.commitIndex;index++{
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		applyMsg := ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied].Command, false, nil}
		applyCh <- applyMsg
		//[12/17] log.Printf("applied log entry %d:%v", rf.lastApplied, rf.log[rf.lastApplied])
		// Apply rf.log[lastApplied] into its state machine
	}
}

// 更新LEADER的commitIndex的函数
func (rf *Raft) updateCommitIndex() {
	// Find logs that has appended to majority and update commitIndex
	for index := rf.commitIndex + 1; index < len(rf.log); index++ {
		// To eliminate problems like the one in Figure 8,
		//  Raft never commits log entries from previous terms by count- ing replicas.
		if rf.log[index].Term < rf.currentTerm {
			continue
		} else if rf.log[index].Term > rf.currentTerm {
			break
		}

		//统计有多少个server拥有该index的log
		commitCounter := 0
		for server := range rf.peers {
			if rf.matchIndex[server] >= index {
				commitCounter++
			}
		}
		// 如果超过一半的server都有该index的log，则更新LEADER的commitIndex
		if rf.isMajority(commitCounter) {
			//[12/17] log.Printf("set commitIndex to %d, matchIndex = %v", index, rf.matchIndex)
			rf.commitIndex = index

			//rf.mu.Lock()
			//rf.apply(rf.applyCh)
			//rf.mu.Unlock()
		}
	}
}

// 修改rf的身份
func (rf *Raft) changeStateTo(newState int) {
	/*if rf.state == FLLOWER && newState == CANDIDATE {

	}
	if rf.state == CANDIDATE && newState == LEADER {
		rf.electionTimeout.Stop()
		rf.heartbeatTimeout.Reset(heartbeatTimeout)
	}
	if rf.state == LEADER && newState == FLLOWER {
		rf.heartbeatTimeout.Stop()
		rf.electionTimeout.Reset(getDuration())
	}*/

	rf.state = newState
}

//lastLogIndex：
//返回rf的log的index
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// 得到随机时间段
func getDuration() time.Duration {
	//duration := time.Duration(electionTimeout + rand.Intn(electionRandomFactor*2) - electionRandomFactor)

	duration := time.Duration(electionTimeout + rand.Intn(electionTimeout+1))
	//Random(-electionRandomFactor, electionRandomFactor))
	return duration
}

// 判断是不是majority（是否超过server数量的一一半）
func (rf *Raft) isMajority(count int) bool {
	if count > len(rf.peers)/2 {
		return true
	} else {
		return false
	}
}

//状态机写法
func (rf *Raft) stateMachine() {
	rand.Seed(time.Now().UnixNano())                      //设置随机种子
	rf.electionTimeout = time.NewTimer(getDuration())     //150ms到300ms之间的随机值
	rf.heartbeatTimeout = time.NewTimer(heartbeatTimeout) //heartbeat的timeout固定为100ms

	for {
		switch rf.state { ///todo 这里应该也要加锁？
		case FLLOWER:
			// 当前的身份是follower
			select {
			case <-rf.electionTimeout.C: //如果follower的election计时器超时
				rf.heartbeatTimeout.Stop()
				rf.electionTimeout.Reset(getDuration())
				// 变成candidate
				rf.changeStateTo(CANDIDATE)

			/*case <-rf.voteCh:
			rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)*/
			default:
			}

		case CANDIDATE:
			// 当前的身份是candidate
			select {
			case <-rf.electionTimeout.C: //如果candidate的election计时器超时
				rf.electionTimeout.Reset(getDuration())
				// 发起新的选举
				rf.mu.Lock()
				rf.elect()
				rf.mu.Unlock()
			default:
			}

		case LEADER:
			// 当前的身份是leader
			select {
			case <-rf.heartbeatTimeout.C: //如果leader的heartbeat计时器超时
				rf.electionTimeout.Stop()
				rf.heartbeatTimeout.Reset(heartbeatTimeout)
				//log.Println("LEADER", rf.me, "触发 heartbeatTimeout")
				// 发送心跳
				rf.mu.Lock()
				rf.heartbeat()
				rf.mu.Unlock()
			default:
			}
		default:
			//log.Print("raft->Make: 不存在这种身份")
			// wrong
		}
	}
}

/*
func (rf *Raft) changeStateTo(newState int) {
	//rf.mu.Lock() ///todo 这个锁放在这里不一定合适，可能放在外面比较好
	//defer rf.mu.Unlock()
	//log.Println("**********", rf.m1[rf.state], rf.me, "from", rf.m1[rf.state], "change to", rf.m1[newState], "**********")
	switch newState {
	case FLLOWER:
		//log.Println(rf.m1[rf.state], rf.me, "term", rf.currentTerm, "身份变为【FLLOWER】")
		rf.state = FLLOWER
		//变成follower之后heartbeatTimeout停止
		rf.heartbeatTimeout.Stop()
		rf.votedFor = -1
		rf.voteCount = 0 // 票数归零///todo 好像不需要
		rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
	case CANDIDATE:
		//log.Println(rf.m1[rf.state], rf.me, "term", rf.currentTerm, "身份变为【CANDIDATE】")
		rf.state = CANDIDATE
		//把electionTimeout重置
		rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
		//成为candidate之后立刻发起选举
		go rf.elect()
	case LEADER:
		////log.Println(rf.m1[rf.state], rf.me, "rf.nextIndex", rf.nextIndex)
		////log.Println(rf.m1[rf.state], rf.me, "rf.log", rf.log)
		//log.Println(rf.m1[rf.state], rf.me, "term", rf.currentTerm, "身份变为【LEADER】")
		rf.state = LEADER
		//变成leader之后electionTimeout停止
		rf.electionTimeout.Stop()
		for server := range rf.nextIndex {
			rf.nextIndex[server] = rf.lastLogIndex() + 1
		}
		for server := range rf.matchIndex {
			rf.matchIndex[server] = 0
		}
		//当选之后立刻发一个空的AppendEntries作为心跳
		//go
		rf.heartbeat()
		//发出心跳之后把heartbeatTimeout重置
		rf.heartbeatTimeout.Reset(100 * time.Millisecond)
		////log.Println(rf.m1[rf.state], rf.me, "rf.nextIndex", rf.nextIndex)
		rf.voteCount = 0 // 票数归零///todo 好像不需要
	default:
		//log.Print("raft->身份切换错误！")
	}
}*/
