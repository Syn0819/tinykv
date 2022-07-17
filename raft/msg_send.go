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
	//log.Infof("node:%v, send RequestVote, to:%v, Index:%v, LogTerm:%v, Term:%v, From:%v",
	//	r.id, to, LastLogIndex, LastLogTerm, r.Term, r.id)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		To:      to,
		Reject:  reject,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		Term:    r.Term,
	}
	//log.Infof("node:%v, sendRequestVoteResponse, To:%v, Reject:%v, From:%v, Term:%v",
	//	r.id, to, reject, r.id, r.Term)
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
	//log.Infof("node:%v, sendAppendEntriesResponse, To:%v, Reject:%v, From:%v, Term:%v, LogTerm:%v, Index:%v",
	//	r.id, to, reject, r.id, r.Term, term, index)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// synchronize the log entries by the log record in leader
	recordLogIndex := r.Prs[to].Next - 1
	log.Infof("node:%v, sendAppend, to:%v, recordLogIndex:%v",
		r.id, to, recordLogIndex)
	recordLogTerm, err := r.RaftLog.Term(recordLogIndex)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		panic(err)
	}

	lens := len(r.RaftLog.entries)
	var entires []*pb.Entry
	for i := int(recordLogIndex - r.RaftLog.firstIndex + 1); i < lens; i++ {
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
	log.Infof("node:%v, sendAppend, to:%v, From:%v, Term:%v, LogTerm:%v, Index:%v, Commit:%v",
		r.id, to, r.id, r.Term, recordLogTerm, recordLogIndex, r.RaftLog.committed)
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

func (r *Raft) sendSnapshot(to uint64) {
	log.Infof("node:%v, sendSnapshot, to:%v", r.id, to)

	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}

	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}
