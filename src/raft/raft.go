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
	"labrpc"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	FLLOWER   = 1
	CANDIDATE = 2
	LEADER    = 3
)

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

// log Entry structure
type Entry struct {
	term    int
	command interface{} ///todo
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd //server编号，从0开始
	persister *Persister
	me        int // index into peers[]

	// Your data here.

	//Persistent state on all servers(Updated on stable storage before responding to RPCs)
	currentTerm int     //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor     int     //candidateId that received vote in current term (or null if none)//当前任期投票给了谁，没投则是-1
	log         []Entry //log entries

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
	electionTimeout  *time.Timer
	heartbeatTimeout *time.Timer

	//提交
	applyCh chan ApplyMsg

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//log.Print("raft->GetState")

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	term         int //candidate’s term
	candidateId  int //candidate requesting vote
	lastLogIndex int //index of candidate’s last log entry
	lastLogTerm  int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	term        int  //currentTerm, for candidate to update itself
	voteGranted bool //true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	//log.Print("raft->RequestVote")
	// Your code here.

	rf.persist() ///todo
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
	//log.Print("raft->sendRequestVote to sever:" + strconv.Itoa(server))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example AppendEntriesArgs RPC arguments structure.
type AppendEntriesArgs struct {
	term         int     //leader’s term
	leaderId     int     //so follower can redirect clients
	prevLogIndex int     //index of log entry immediately preceding new ones
	prevLogTerm  int     //term of prevLogIndex entry
	entries      []Entry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	leaderCommit int     //leader’s commitIndex
}

// example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	term    int  //currentTerm, for leader to update itself
	success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//log.Print("raft->AppendEntriesArgs to sever:" + strconv.Itoa(server))
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
	//log.Print("raft->Start")

	var term int
	var index int
	var isLeader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == LEADER {
		isLeader = true
		newEntry := Entry{command: command, term: term}
		rf.log = append(rf.log, newEntry) //leader增加新条目
		//持久化
		rf.persist() ///todo 持久化应该放在哪里？
		index = len(rf.log) - 1
		//更新自己
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
	} else {
		isLeader = false
		index = -1
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
	//log.Print("raft->Kill")
	// Your code here, if desired.
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
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
	//log.Print("raft->Make")

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	//Persistent state on all servers
	rf.currentTerm = 0        //初始term为0
	rf.voteFor = -1           //初始没有投票
	rf.log = make([]Entry, 1) //0位置放置一个空条目，后续的条目index从1开始

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	//rf.readPersist

	//Volatile state on all servers
	rf.commitIndex = 0 //初始committed为0
	rf.lastApplied = 0 //初始entry applied为0
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

	rf.state = FLLOWER                                                                       //初始身份都是follower
	rand.Seed(time.Now().UnixNano())                                                         //设置随机种子
	rf.electionTimeout = time.NewTimer(time.Duration(rand.Intn(151)+150) * time.Millisecond) //150ms到300ms之间的随机值
	rf.heartbeatTimeout = time.NewTimer(100 * time.Millisecond)                              //heartbeat的timeout固定为100ms

	rf.applyCh = applyCh ///todo ?

	// 异步执行循环
	go func() {
		for {
			switch rf.state { ///todo 这里应该也要枷锁
			case FLLOWER:
				// 当前的身份是follower
				select {
				case <-rf.electionTimeout.C: //如果follower的election计时器超时
					// 变成candidate
					rf.mu.Lock()
					rf.state = CANDIDATE
					rf.mu.Unlock()
					// 立即发起选举
					rf.elect()
					rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
				default:
				}

			case CANDIDATE:
				// 当前的身份是candidate
				select {
				case <-rf.electionTimeout.C: //如果candidate的election计时器超时
					// 发起新的选举
					rf.elect()
					rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
				default:
				}

			case LEADER:
				// 当前的身份是leader
				select {
				case <-rf.heartbeatTimeout.C: //如果leader的heartbeat计时器超时
					// 发送心跳
					rf.heartbeat()
				default:
				}
			default:
				//log.Print("raft->Make: 不存在这种身份")
				// wrong
			}
		}
	}()

	return rf
}

// 选举
func (rf *Raft) elect() {
	//log.Print("raft->elect")

	rf.mu.Lock()       ///TODO 这个锁应该加在哪里？
	rf.currentTerm++   //term++
	rf.voteFor = rf.me //投票给自己
	rf.voteCount = 1   ///TODO 自己的票算不算？
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me {
			go func(server int) { //另起一个线程来sendRequestVote
				//log.Println("new thread to sendRequestVote")
				requestVoteArgs := RequestVoteArgs{}
				var requestVoteReply RequestVoteReply
				rf.mu.Lock()
				requestVoteArgs.term = rf.currentTerm
				requestVoteArgs.candidateId = rf.me

				requestVoteArgs.lastLogIndex = rf.lastLogIndex()
				log.Println("requestVoteArgs.lastLogIndex = " + strconv.Itoa(requestVoteArgs.lastLogIndex))
				requestVoteArgs.lastLogTerm = rf.log[requestVoteArgs.lastLogIndex].term

				rf.mu.Unlock()

				ok := rf.sendRequestVote(server, requestVoteArgs, &requestVoteReply)
				if ok {
					rf.mu.Lock()
					//log.Print("sendRequestVote success")
					//如果发现自己的term小，则退回follower
					if rf.currentTerm < requestVoteReply.term && rf.state != FLLOWER {
						rf.currentTerm = requestVoteReply.term
						rf.changeStateTo(FLLOWER) ///todo
						//persist
					}
					//如果仍是candadite身份并且获得投票
					if rf.state == CANDIDATE && requestVoteReply.voteGranted {
						//log.Print("voteGranted true")
						rf.voteCount++
						if rf.voteCount > len(rf.peers)/2 { //如果获得多数票则当选
							//log.Print("become leader: " + strconv.Itoa(rf.me))
							rf.changeStateTo(LEADER) // 身份变为leader
							//rf.heartbeat()
						} else {
							////log.Print("become leader: ", rf.me)
						}
					} else {
						//log.Print("voteGranted false")
					}
					rf.mu.Unlock()
				} else {
					//log.Print("sendRequestVote fail")
				}
			}(server)
		}
	}
}

// 心跳
func (rf *Raft) heartbeat() {
	//log.Print("raft->heartbeat")
	for server, _ := range rf.peers {
		if server != rf.me {
			go func(server int) {
				///todo if rf.state!=LEADER
				appendEntriesArgs := AppendEntriesArgs{}
				var appendEntriesReply AppendEntriesReply
				rf.mu.Lock()
				appendEntriesArgs.term = rf.currentTerm
				appendEntriesArgs.leaderId = rf.me
				appendEntriesArgs.prevLogIndex = rf.nextIndex[server] - 1 ///
				appendEntriesArgs.prevLogTerm = rf.log[appendEntriesArgs.prevLogIndex].term
				appendEntriesArgs.entries = make([]Entry, 0) ///heartbeat的条目为空///todo
				appendEntriesArgs.leaderCommit = rf.commitIndex
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(server, appendEntriesArgs, &appendEntriesReply)

				if ok {
					//log.Print("sendAppendEntries success")
					if appendEntriesReply.success {
						//log.Print("appendEntriesReply true")
						/// todo

					} else {
						//log.Print("appendEntriesReply false")
						// 如果leader的term比对方的小
						if rf.currentTerm < appendEntriesReply.term {
							//log.Print("rf.currentTerm < appendEntriesReply.term")
							//将leader的term改为对方的term
							rf.currentTerm = appendEntriesReply.term
							//身份退回follower
							rf.changeStateTo(FLLOWER)
						} else {
							//log.Print("rf.currentTerm >= appendEntriesReply.term")
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

func (rf *Raft) changeStateTo(newState int) {
	switch newState {
	case FLLOWER:
		//变成follower之后heartbeatTimeout停止
		rf.heartbeatTimeout.Stop()
		rf.voteFor = -1
		rf.voteCount = 0 // 票数归零///todo 好像不需要
		rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
	case CANDIDATE:
		//成为candidate之后立刻发起选举
		rf.elect()
		//发起选举之后把electionTimeout重置
		rf.electionTimeout.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
	case LEADER:
		//变成leader之后electionTimeout停止
		rf.electionTimeout.Stop()
		for server := range rf.nextIndex {
			rf.nextIndex[server] = rf.lastLogIndex()
		}
		for server := range rf.matchIndex {
			rf.matchIndex[server] = 0
		}
		rf.voteCount = 0 // 票数归零///todo 好像不需要
		//当选之后立刻发一个空的AppendEntries作为心跳
		rf.heartbeat()
		//发出心跳之后把heartbeatTimeout重置
		rf.heartbeatTimeout.Reset(100 * time.Millisecond)
	default:
		//log.Print("raft->身份切换错误！")
	}
}
