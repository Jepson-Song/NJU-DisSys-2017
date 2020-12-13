package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	//log.Print("persister->MakePersister")
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	//log.Print("persister->Copy")
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(data []byte) {
	//log.Print("persister->SaveRaftState")
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = data
}

func (ps *Persister) ReadRaftState() []byte {
	//log.Print("persister->ReadRaftState")
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	//log.Print("persister->RaftStateSize")
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) SaveSnapshot(snapshot []byte) {
	//log.Print("persister->SaveSnapshot")
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	//log.Print("persister->ReadSnapshot")
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}
