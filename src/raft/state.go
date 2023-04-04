package raft

import "fmt"

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

func (r *Role) toString() string {
	switch *r {
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
	return rf.currentTerm
}
func (rf *Raft) setCurrentTerm(term int) {
	if term < rf.currentTerm {
		panic(fmt.Sprintf("Term %d is less than current term %d", term, rf.currentTerm))
	}
	DTrace("%s set term to %d", rf.getServerInfo().toString(), term)
	rf.currentTerm = term
}
func (rf *Raft) addTerm() {
	DTrace("%s start next term", rf.getServerInfo().toString())
	rf.currentTerm += 1
}

func (rf *Raft) getRole() Role {
	return Role(rf.role)
}
func (rf *Raft) setRole(r Role) {
	rf.role = r
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
	return fmt.Sprintf("[%9s] [%d] [Term %2d]", rf.role.toString(), rf.me, rf.currentTerm)
}

type ServerInfo struct {
	Term     int
	Role     Role
	ServerId int
}

func (rf *Raft) getServerInfo() ServerInfo {
	return ServerInfo{
		rf.currentTerm,
		rf.role,
		rf.me,
	}
}
func (rf ServerInfo) toString() string {
	return fmt.Sprintf("[%9s] [%d] [Term %d]", rf.Role.toString(), rf.ServerId, rf.Term)
}
