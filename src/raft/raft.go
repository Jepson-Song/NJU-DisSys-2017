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
	"log"
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
	electionTimeout      = 400 // ms
	electionRandomFactor = 100 // ms
	heartbeatTimeout     = 150 // ms
	nilIndex             = -1
)

// 先声明map

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
	votedFor int        //CandidateId that received vote in current Term (or null if none)//当前任期投票给了谁，没投则是-1
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
	applyCh chan ApplyMsg ///

	//成功投票
	voteCh chan struct{}

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
	LastLogTerm  int //Term of candidate’s last log entry
	LastLogIndex int //index of candidate’s last log entry

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

// compareLog :
//	return true if A is older(outdated) than B
func compareLog(lastTermA int, lastIndexA int, lastTermB int, lastIndexB int) bool {
	if lastTermA != lastTermB {
		return lastTermA < lastTermB
	}
	return lastIndexA < lastIndexB
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
	isCandidateOutdated := compareLog(args.LastLogTerm,
		args.LastLogIndex, rf.log[lastLogIndex].Term, lastLogIndex)
	//如果args的不是最新的
	if (args.LastLogTerm < rf.log[lastLogIndex].Term) ||
		(args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex < lastLogIndex) {
		//log.Println("<<< server", rf.me, "term", rf.currentTerm, "不投票")
		reply.VoteGranted = false
		/*******可以删除***************/
		reason := ""
		if rf.currentTerm > args.Term {
			reason = "RPC term is outdated"
		} else if isCandidateOutdated {
			reason = "Candidate's log is outdated"
		}
		log.Printf("refuse to vote for Node %d(Term %d), %s", args.CandidateId, args.Term, reason)
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
		log.Printf("accept vote for Node %d(Term %d), %v",
			args.CandidateId, args.Term, []int{args.LastLogTerm, args.LastLogIndex, rf.log[lastLogIndex].Term, lastLogIndex})

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
	if rf.currentTerm > args.Term || args.LeaderID == rf.me { // if the rpc is outdated, ignore it
		// log.Printf("Node %d(Term %d) ignored append from leader %d(Term %d) after entry {%d, %d}",
		//   rf.me, rf.currentTerm, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		return
	}
	rf.currentTerm = reply.Term
	if rf.state != FLLOWER {
		rf.changeStateTo(FLLOWER)
	}
	/*
		if rf.currentTerm <= args.Term {
			//log.Println(rf.m1[rf.state], rf.me, "Term", rf.currentTerm, "<= server", args.LeaderID, "Term", args.Term)
			rf.votedFor = -1
			//todo
			rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
			if rf.currentTerm < args.Term && rf.state == LEADER {
				rf.heartbeatTimeout.Stop()
			}
			if rf.state != FLLOWER {
				rf.mu.Lock()
				rf.changeStateTo(FLLOWER)
				rf.mu.Unlock()
			}

			reply.Success = true

			////log.Println(rf.m1[rf.state], rf.me, "Term", rf.currentTerm, "electionTimeout.Reset")

		} else {
			//log.Println(rf.m1[rf.state], rf.me, "Term", rf.currentTerm, "> server", args.LeaderID, "Term", args.Term)

			reply.Success = false
		}

		reply.Success = true
	*/

	/*****************/
	if len(rf.log) <= args.PrevLogIndex || // if the log doesn't contain prevLogIndex
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // if the log entry doesn't match the prev log
		log.Printf("refuse to append from leader %d(Term %d) after entry {%d, %d}, log not match",
			args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		if args.PrevLogIndex < len(rf.log) {
			conflictTerm := rf.log[args.PrevLogIndex].Term
			i := args.PrevLogIndex
			for ; i > 0; i-- {
				if rf.log[i].Term != conflictTerm {
					break
				}
			}
			reply.ConflictIndex = i + 1
		} else {
			reply.ConflictIndex = len(rf.log)
		}
	} else {
		reply.Success = true

		if len(args.Entries) > 0 {
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			rf.persist()
		}

		// update commitIndex
		if rf.commitIndex < args.LeaderCommit {
			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
		}

		if len(args.Entries) > 0 {
			log.Printf("accept append %d entries from leader %d(Term %d) from entry {%d, %d}; commited = %d, leaderCommit = %d, logLen = %d, lastTerm = %d",
				len(args.Entries), args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm,
				rf.commitIndex, args.LeaderCommit, len(rf.log), rf.log[len(rf.log)-1].Term)
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

	/*var term int
	var index int
	var isLeader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == LEADER {
		isLeader = true

		newLogEntry := LogEntry{Command: command, Term: term}
		rf.log = append(rf.log, newLogEntry) //leader增加新条目

		//持久化
		//rf.persist() ///todo 持久化应该放在哪里？

		//index = len(rf.log) - 1
		//更新自己
		lastLogIndex := rf.lastLogIndex()
		rf.nextIndex[rf.me] = lastLogIndex + 1
		rf.matchIndex[rf.me] = lastLogIndex
		index = lastLogIndex
	} else {
		isLeader = false
		index = -1
	}*/
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
	log.Printf("is killed")
}

//lastLogIndex：
//返回rf的log的index
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
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
	/*rf.m1 = make(map[int]string)
	rf.m1[1] = "FOLLOWER"
	rf.m1[2] = "CANDIDATE"
	rf.m1[3] = "LEADER"

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
	log.Printf("Initialize")

	// All servers
	go func() {
		for {
			/*if rf.isKilled {
				return
			}*/

			rf.mu.Lock()
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				applyMsg := ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied].Command, false, nil}
				applyCh <- applyMsg
				log.Printf("applied log entry %d:%v", rf.lastApplied, rf.log[rf.lastApplied])
				// Apply rf.log[lastApplied] into its state machine
			}
			rf.mu.Unlock()

			time.Sleep(50 * time.Millisecond)
		}
	}()
	/*//可能会导致backup过不了
	duration := time.Duration(electionTimeout + rand.Intn(electionRandomFactor*2) - electionRandomFactor)
	//Random(-electionRandomFactor, electionRandomFactor))
	time.Sleep(duration * time.Millisecond)
	*/
	// candidate thread
	go func() {
		var counterLock sync.Mutex ///删掉之后backup有可能过不了
		for {
			/*if rf.isKilled {
				return
			}*/

			rf.mu.Lock()
			if rf.state == FLLOWER { // ONLY follower would have election timeout
				//rf.state = CANDIDATE
				rf.changeStateTo(CANDIDATE)
				//rf.currentTerm++//变成candidate之后不能立刻修改term，不然会出错
			}
			rf.mu.Unlock()

			//CANDIDATE等待一段时间之后发起选举
			duration := time.Duration(electionTimeout + rand.Intn(electionRandomFactor*2) - electionRandomFactor)
			//Random(-electionRandomFactor, electionRandomFactor))
			time.Sleep(duration * time.Millisecond)

			rf.mu.Lock()

			if rf.state == CANDIDATE {
				log.Printf("start to request votes for term %d", rf.currentTerm+1)
				counter := 0
				//lastTerm := 0
				//logLen := len(rf.log)
				/*if logLen > 0 {
					lastTerm = rf.log[logLen-1].Term
				}*/
				lastIndex := rf.lastLogIndex() //logLen - 1
				requestTerm := rf.currentTerm + 1
				lastTerm := rf.log[lastIndex].Term
				requestVoteArgs := RequestVoteArgs{requestTerm, rf.me, lastTerm, lastIndex}
				requestVoteReply := make([]RequestVoteReply, len(rf.peers))

				/*var requestVoteArgs RequestVoteArgs
				requestVoteArgs.Term = rf.currentTerm + 1
				requestVoteArgs.CandidateId = rf.me
				lastLogIndex := rf.lastLogIndex()
				requestVoteArgs.LastLogTerm = rf.log[lastLogIndex].Term
				requestVoteArgs.LastLogIndex = lastLogIndex*/

				for index := range rf.peers {
					go func(index int) {
						ok := rf.sendRequestVote(index, requestVoteArgs, &requestVoteReply[index])
						rf.mu.Lock()
						if requestVoteReply[index].Term > rf.currentTerm {
							rf.currentTerm = requestVoteReply[index].Term
							//rf.state = FLLOWER
							rf.changeStateTo(FLLOWER)
							rf.persist()
						} else if ok && (requestVoteArgs.Term == rf.currentTerm) && requestVoteReply[index].VoteGranted {
							counterLock.Lock()
							counter++
							if counter > len(rf.peers)/2 && rf.state != LEADER {
								rf.state = LEADER
								rf.currentTerm = requestTerm
								rf.nextIndex = make([]int, len(rf.peers))
								rf.matchIndex = make([]int, len(rf.peers))
								// immediately send heartbeats to others to stop election
								for i := range rf.peers {
									rf.nextIndex[i] = len(rf.log)
								}
								rf.persist()

								log.Printf("become leader for term %d, nextIndex = %v, requestVoteArgs = %v", rf.currentTerm, rf.nextIndex, requestVoteArgs)
							}
							counterLock.Unlock()
						}
						rf.mu.Unlock()
					}(index)
				}
			}
			rf.mu.Unlock()
		}
	}()

	// leader thread
	go func() {
		for {
			/*if rf.isKilled {
				return
			}*/
			time.Sleep(heartbeatTimeout * time.Millisecond)
			rf.mu.Lock()
			// send AppendEntries(as heartbeats) RPC
			if rf.state == LEADER {
				currentTerm := rf.currentTerm
				for index := range rf.peers {
					go func(index int) {
						// decrease rf.nextIndex[index] in loop till append success
						for {
							if index == rf.me || rf.state != LEADER {
								break
							}
							// if rf.nextIndex[index] <= 0 || rf.nextIndex[index] > len(rf.log){
							//   log.Printf("Error: rf.nextIndex[%d] = %d, logLen = %d", index, rf.nextIndex[index], len(rf.log))
							// }
							rf.mu.Lock()
							logLen := len(rf.log)
							appendEntries := rf.log[rf.nextIndex[index]:]
							prevIndex := rf.nextIndex[index] - 1
							aeArgs := AppendEntriesArgs{currentTerm, rf.me,
								prevIndex, rf.log[prevIndex].Term,
								appendEntries, rf.commitIndex}
							aeReply := AppendEntriesReply{}
							rf.mu.Unlock()

							ok := rf.sendAppendEntries(index, aeArgs, &aeReply)
							rf.mu.Lock()
							if ok && rf.currentTerm == aeArgs.Term { // ensure the reply is not outdated
								if aeReply.Success {
									rf.matchIndex[index] = logLen - 1
									rf.nextIndex[index] = logLen
									rf.mu.Unlock()
									break
								} else {
									if aeReply.Term > rf.currentTerm { // this leader node is outdated
										rf.currentTerm = aeReply.Term
										//rf.state = FLLOWER
										rf.changeStateTo(FLLOWER)
										rf.persist()
										rf.mu.Unlock()
										break
									} else { // prevIndex not match, decrease prevIndex
										// rf.nextIndex[index]--
										// if aeReply.ConflictIndex<= 0 || aeReply.ConflictIndex>= logLen{
										//   log.Printf("Error: aeReply.ConflictIndexfrom %d = %d, logLen = %d", aeReply.ConflictFromIndex, index, logLen)
										// }
										rf.nextIndex[index] = aeReply.ConflictIndex
									}
								}
							}
							rf.mu.Unlock()
						}
					}(index)
				}

				// Find logs that has appended to majority and update commitIndex
				for N := rf.commitIndex + 1; N < len(rf.log); N++ {
					// To eliminate problems like the one in Figure 8,
					//  Raft never commits log entries from previous terms by count- ing replicas.
					if rf.log[N].Term < rf.currentTerm {
						continue
					} else if rf.log[N].Term > rf.currentTerm {
						break
					}
					followerHas := 0
					for index := range rf.peers {
						if rf.matchIndex[index] >= N {
							followerHas++
						}
					}
					// If majority has the log entry of index N
					if followerHas > len(rf.peers)/2 {
						log.Printf("set commitIndex to %d, matchIndex = %v", N, rf.matchIndex)
						rf.commitIndex = N
					}
				}
			}
			rf.mu.Unlock()
		}
	}()
	return rf ///
}

func (rf *Raft) stateMachine() {
	for {
		switch rf.state { ///todo 这里应该也要加锁？
		case FLLOWER:
			// 当前的身份是follower
			select {
			case <-rf.electionTimeout.C: //如果follower的election计时器超时
				////log.Println("FLLOWER", rf.me, "触发 electionTimeout")
				// 变成candidate
				rf.mu.Lock()
				rf.changeStateTo(CANDIDATE)
				rf.mu.Unlock()
				// 立即发起选举
				//rf.elect()

				rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
			case <-rf.voteCh:
				rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
			default:
			}

		case CANDIDATE:
			// 当前的身份是candidate
			select {
			case <-rf.electionTimeout.C: //如果candidate的election计时器超时
				////log.Println("CANDIDATE", rf.me, "触发 electionTimeout")
				// 发起新的选举
				rf.mu.Lock()
				rf.elect()
				rf.mu.Unlock()
				rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
			default:
			}

		case LEADER:
			// 当前的身份是leader
			select {
			case <-rf.heartbeatTimeout.C: //如果leader的heartbeat计时器超时
				////log.Println("LEADER", rf.me, "触发 heartbeatTimeout")
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

// 选举
func (rf *Raft) elect() {
	//log.Println(rf.m1[rf.state], rf.me, "开始选举...")
	//requestVoteArgs := &RequestVoteArgs{0, 0, 0, 0}
	//rf.mu.Lock()
	//rf.mu.Lock()       ///TODO 这个锁应该加在哪里？
	rf.currentTerm++    //Term++
	rf.votedFor = rf.me //投票给自己
	rf.voteCount = 0    ///TODO 自己的票算不算？
	//rf.mu.Unlock()

	var requestVoteArgs RequestVoteArgs
	//requestVoteArgs := RequestVoteArgs{}
	requestVoteArgs.Term = rf.currentTerm
	//requestVoteArgs.from = rf.me
	requestVoteArgs.CandidateId = rf.me
	////log.Println(rf.m1[rf.state], rf.me, "Term", rf.currentTerm, "和它发出的args CANDIDATE", requestVoteArgs.CandidateId, "Term", requestVoteArgs.Term)

	requestVoteArgs.LastLogIndex = rf.lastLogIndex()
	////log.Println(rf.m1[rf.state], rf.me, "requestVoteArgs.LastLogIndex = "+strconv.Itoa(requestVoteArgs.LastLogIndex))
	requestVoteArgs.LastLogTerm = rf.log[requestVoteArgs.LastLogIndex].Term

	//rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me {
			////log.Println(rf.m1[rf.state], rf.me, "Term", rf.currentTerm, "准备向", "server", server, "sendRequestVote")
			go func(server int) { //另起一个线程来sendRequestVote
				////log.Println("new thread to sendRequestVote")
				var requestVoteReply RequestVoteReply

				ok := rf.sendRequestVote(server, requestVoteArgs, &requestVoteReply)

				rf.mu.Lock()
				defer rf.mu.Unlock() ///todo 这个锁好像会造成死锁
				if ok {
					////log.Println(rf.m1[rf.state], rf.me, "sendRequestVote Success")
					//如果发现自己的Term小，则退回follower
					if rf.currentTerm < requestVoteReply.Term {
						////log.Println(rf.m1[rf.state], rf.me, "Term", rf.currentTerm, "reply Term", reply.Term, "所以退回follower")
						rf.currentTerm = requestVoteReply.Term
						if rf.state != FLLOWER {
							rf.changeStateTo(FLLOWER) ///todo
						}
						//persist
					}
					//如果仍是candadite身份并且获得投票
					if rf.state == CANDIDATE && requestVoteReply.VoteGranted {
						////log.Println(rf.m1[rf.state], rf.me, "voteGranted true")
						//log.Print("voteGranted true")
						rf.voteCount++
						if rf.voteCount > len(rf.peers)/2 { //如果获得多数票则当选
							////log.Println(rf.m1[rf.state], rf.me, "become LEADER")
							//log.Print("become leader: " + strconv.Itoa(rf.me))
							rf.changeStateTo(LEADER) // 身份变为leader
							//rf.heartbeat()
						} else {
							////log.Print("become leader: ", rf.me)
						}
					} else {
						////log.Println(rf.m1[rf.state], rf.me, "voteGranted false")
						//log.Print("voteGranted false")
					}
				} else {
					//log.Print("sendRequestVote fail")
				}
			}(server)
		}
	}
}

// 心跳
func (rf *Raft) heartbeat() {
	//log.Println(rf.m1[rf.state], rf.me, "开始发送心跳...")
	////log.Println(rf.m1[rf.state], rf.me, "rf.nextIndex", rf.nextIndex)
	//rf.mu.Lock()
	//me := rf.me
	//defer rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me {
			go func(server int) {
				////log.Println("@@@ ", rf.m1[rf.state], rf.me, "向", "server", server, "发送心跳")
				///todo if rf.state!=LEADER
				rf.mu.Lock()
				appendEntriesArgs := AppendEntriesArgs{}
				appendEntriesArgs.Term = rf.currentTerm
				appendEntriesArgs.LeaderID = rf.me
				appendEntriesArgs.PrevLogIndex = rf.nextIndex[server] - 1 ///
				////log.Println("@@@ appendEntriesArgs.PrevLogIndex", appendEntriesArgs.PrevLogIndex)
				appendEntriesArgs.PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex].Term
				appendEntriesArgs.Entries = make([]LogEntry, 0) ///heartbeat的条目为空///todo
				appendEntriesArgs.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
				var appendEntriesReply AppendEntriesReply

				ok := rf.sendAppendEntries(server, appendEntriesArgs, &appendEntriesReply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok {
					//log.Print("sendAppendEntries Success")
					if appendEntriesReply.Success {
						//log.Print("appendEntriesReply true")
						/// todo
						if rf.currentTerm < appendEntriesReply.Term {
							rf.currentTerm = appendEntriesReply.Term
							rf.changeStateTo(FLLOWER)
						} else {
							/// TODO
						}

					} else {
						//log.Print("appendEntriesReply false")
						// 如果leader的Term比对方的小
						if rf.currentTerm < appendEntriesReply.Term {
							//log.Print("rf.currentTerm < appendEntriesReply.Term")
							//将leader的Term改为对方的Term
							rf.currentTerm = appendEntriesReply.Term
							//身份退回follower
							//rf.mu.Lock()
							rf.changeStateTo(FLLOWER)
							//rf.mu.Unlock()
						} else {
							//log.Print("rf.currentTerm >= appendEntriesReply.Term")
							/// TODO
						}
					}

				} else {
					//log.Print("sendAppendEntries fail")
				}
			}(server)
		}
	}
	rf.heartbeatTimeout.Reset(100 * time.Millisecond) //heartbeatTimeout开始///?
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

func (rf *Raft) changeStateTo(newState int) {
	rf.state = newState
}
