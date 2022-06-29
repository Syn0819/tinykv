package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

//
func (r *Raft) sendRequestVote(to, LastLogIndex, LastLogTerm uint64) {
	msg := pb.Message{
		To:      to,
		Index:   LastLogIndex,
		LogTerm: LastLogTerm,
		Term:    r.Term,
		From:    r.id,
		MsgType: pb.MessageType_MsgRequestVote,
	}
	r.msgs = append(r.msgs, msg)
	log.Infof("node:%v, send RequestVote, to:%v", r.id, to)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		To:      to,
		Reject:  reject,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendAppendEntriesResponse(to uint64, reject bool, index uint64, term uint64) {
	msg := pb.Message{
		To:      to,
		Reject:  reject,
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// synchronize the log entries by the log record in leader
	recordLogIndex := r.Prs[to].Next
	recordLogTerm, err := r.RaftLog.Term(recordLogIndex)
	if err != nil {
		panic(err)
	}

	lens := len(r.RaftLog.entries)
	var entires []*pb.Entry
	for i := recordLogIndex - r.RaftLog.firstIndex + 1; i < uint64(lens); i++ {
		entires = append(entires, &r.RaftLog.entries[i])
	}

	msg := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		Term:    r.Term,
		LogTerm: recordLogTerm,
		Index:   recordLogIndex,
		Entries: entires,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	log.Infof("node:%v, sendAppend, to:%v", r.id, to)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		To:      to,
		Reject:  reject,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}
