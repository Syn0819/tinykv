// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// logc replication progress of eah peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	transferElapsed int
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 即将处理的conf index，该值是唯一的，只有当leader的applied index大于该值时该日志，会进行处理
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	nRaft := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		votes:            make(map[uint64]bool),
	}

	nRaft.becomeFollower(0, None)
	nRaft.randomElectionTimeout = nRaft.electionTimeout + rand.Intn(nRaft.electionTimeout)
	// paramters read from storage
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	nRaft.Term = hardState.GetTerm()
	nRaft.Vote = hardState.GetVote()
	nRaft.RaftLog.committed = hardState.GetCommit()
	if c.Applied > 0 {
		nRaft.RaftLog.applied = c.Applied
	}

	if c.peers == nil {
		c.peers = confState.Nodes
	}

	lastLogIndex := nRaft.RaftLog.lastIndex
	for _, peer := range c.peers {
		if peer == nRaft.id {
			nRaft.Prs[peer] = &Progress{Next: lastLogIndex + 1, Match: lastLogIndex}
		} else {
			nRaft.Prs[peer] = &Progress{Next: lastLogIndex + 1, Match: 0}
		}
	}
	return nRaft
}

// checkSendElection check whether the node need to start election
func (r *Raft) checkSendElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// checkSendHeartbeat check whether the leader need to send heart beat to peers
func (r *Raft) checkSendHeartbeat() {
	r.heartbeatElapsed++
	// log.Infof("node:%v, checkSendHeartbeat, heartbeatElapsed:%v, heartbeatTimeout:%v",
	//	r.id, r.heartbeatElapsed, r.heartbeatTimeout)
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}

}

func (r *Raft) tickTransfer() {
	r.transferElapsed++
	if r.transferElapsed >= r.electionTimeout*2 {
		r.transferElapsed = 0
		r.leadTransferee = None
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.checkSendElection()
	case StateCandidate:
		r.checkSendElection()
	case StateLeader:
		if r.leadTransferee != None {
			r.tickTransfer()
		}
		r.checkSendHeartbeat()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
	log.Infof("node:%v, becomeFollower", r.id)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	r.State = StateCandidate
	r.Lead = None

	r.votes = make(map[uint64]bool)
	// vote itself
	r.votes[r.id] = true
	log.Infof("node:%v, becomeCandidate", r.id)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// according to the raft paper, the Prs should reinitialized after election
	lastLogIndex := r.RaftLog.LastIndex()

	// noop entry
	entry := pb.Entry{
		Term:  r.Term,
		Index: lastLogIndex + 1,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, entry)

	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Next = lastLogIndex + 2
			r.Prs[peer].Match = lastLogIndex + 1
		} else {
			r.Prs[peer].Next = lastLogIndex + 1
			r.Prs[peer].Match = 0
		}
	}

	r.broadcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
	log.Infof("node:%v, becomeLeader", r.id)
}

func (r *Raft) followerMsgHandle(m pb.Message) {
	// log.Infof("nodo:%v, followerMsgHandle, m.MsgType:%v, m.From:%v, m.To:%v, m.Term:%v, m.Commit:%v",
	//	r.id, m.MsgType, m.From, m.To, m.Term, m.Commit)
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleElection()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleElection()
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
}

func (r *Raft) candidateMsgHandle(m pb.Message) {
	// log.Infof("nodo:%v, candidateMsgHandle, m.MsgType:%v, m.From:%v, m.To:%v, m.Term:%v, m.Commit:%v",
	// 	r.id, m.MsgType, m.From, m.To, m.Term, m.Commit)
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleElection()
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, r.Term)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
}

func (r *Raft) leaderMsgHandle(m pb.Message) {
	//log.Infof("nodo:%v, leaderMsgHandle, m.MsgType:%v, m.From:%v, m.To:%v, m.Term:%v, m.Commit:%v",
	//	r.id, m.MsgType, m.From, m.To, m.Term, m.Commit)
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
		r.sendAppend(m.From)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// Rules for servers: All servers 2
	log.Infof("node:%v, Step, MsgType:%v, r.State:%v, r.Term:%v, m.Term:%v",
		r.id, m.MsgType, r.State, r.Term, m.Term)
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		r.followerMsgHandle(m)
	case StateCandidate:
		r.candidateMsgHandle(m)
	case StateLeader:
		r.leaderMsgHandle(m)
	}
	return nil
}

func (r *Raft) Commit() {
	// check whether log needed to be commit, which quorum confirm
	lens := len(r.Prs)
	allMatchIndex := make(uint64Slice, lens)
	log.Infof("Commit, lens: %d, allMatchIndex: %d", lens, allMatchIndex)
	i := 0
	for _, peer := range r.Prs {
		allMatchIndex[i] = peer.Match
		i++
	}

	sort.Sort(allMatchIndex)
	if allMatchIndex[(lens-1)/2] > r.RaftLog.committed {
		// have new log which needed to commit
		// section 5.4.2
		logTerm, err := r.RaftLog.Term(allMatchIndex[(lens-1)/2])
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = allMatchIndex[(lens-1)/2]
			r.broadcastAppend()
		}
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	log.Infof("addNode, id: %d", id)
	if _, ok := r.Prs[id]; !ok {
		log.Infof("do not find the node id:%v, add to r.Prs", id)
		r.Prs[id] = &Progress{Next: 1}
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	log.Infof("removeNode, id: %d", id)
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		//log.Infof("removeNode, r.Prs: %d", r.Prs)
		if r.State == StateLeader {
			r.Commit()
		}
	}
	r.PendingConfIndex = None
}
