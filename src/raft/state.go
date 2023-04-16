package raft

import "fmt"

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

func (r Role) toString() string {
	switch r {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	}
	return "Unknown"
}

type RaftState struct {
	currentTerm int
	role        Role
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	return term
}
func (rf *Raft) setCurrentTerm(term int) {
	if term < rf.getCurrentTerm() {
		rf.mu.Unlock()
		panic(fmt.Sprintf("Term %d is less than current term %d", term, rf.currentTerm))
	}
	DTrace("%s set term to %d", rf.getServerInfo().toString(), term)
	rf.mu.Lock()
	rf.currentTerm = term
	rf.mu.Unlock()
}
func (rf *Raft) addTerm() {
	DTrace("%s start next term", rf.getServerInfo().toString())
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.mu.Unlock()
}

func (rf *Raft) getRole() Role {
	rf.mu.Lock()
	role := Role(rf.role)
	rf.mu.Unlock()
	return role
}
func (rf *Raft) setRole(r Role) {
	rf.mu.Lock()
	rf.role = r
	rf.mu.Unlock()
}
func (rf *Raft) isLeader() bool {
	return rf.getRole() == Leader
}
func (rf *Raft) isCandidate() bool {
	return rf.getRole() == Candidate
}
func (rf *Raft) isFollower() bool {
	return rf.getRole() == Follower
}

func (rf *Raft) toString() string {
	return fmt.Sprintf("[%9s] [%d] [Term %2d]", rf.getRole().toString(), rf.me, rf.getCurrentTerm())
}

type ServerInfo struct {
	Term     int
	Role     Role
	ServerId int
}

func (rf *Raft) getServerInfo() ServerInfo {
	return ServerInfo{
		rf.getCurrentTerm(),
		Role(rf.getCurrentTerm()),
		rf.me,
	}
}
func (rf ServerInfo) toString() string {
	return fmt.Sprintf("[%9s] [%d] [Term %d]", rf.Role.toString(), rf.ServerId, rf.Term)
}

func (rf *Raft) resetVote() {
	rf.mu.Lock()
	rf.votedFor = nil
	rf.mu.Unlock()
}
