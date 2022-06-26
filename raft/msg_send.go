package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

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
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}
