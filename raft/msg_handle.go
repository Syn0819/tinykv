package raft

import (
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) handleElection() {
	r.becomeCandidate()

	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	// only one node, do not need election
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	LastLogIndex := r.RaftLog.LastIndex()
	LastLogTerm, err := r.RaftLog.Term(LastLogIndex)
	if err != nil {
		panic(err)
	}

	for peer := range r.Prs {
		if peer == r.id {
			continue
		} else {
			r.sendRequestVote(peer, LastLogIndex, LastLogTerm)
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// if the node already vote to one node, rejcet
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	// if the node current term > candidate term, reject
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	//log.Infof("node:%v, handleRequestVote, lastLogIndex:%v, lastLogTerm:%v, m.From:%v, m.Term:%v, m.Index:%v, m.Commit:%v",
	//	r.id, lastLogIndex, lastLogTerm, m.From, m.Term, m.Index, m.Commit)
	if err != nil {
		panic(err)
	}

	if lastLogTerm > m.LogTerm {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if lastLogTerm == m.LogTerm && lastLogIndex > m.Index {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	// accpet!
	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendRequestVoteResponse(m.From, false)

}

// handleRequestVoteResponse handle handleRequestVoteResponse RPC request
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	r.votes[m.From] = !m.Reject
	curVoteLens := len(r.votes)
	peerLens := len(r.Prs)
	quorum := peerLens / 2

	if curVoteLens < quorum {
		return
	} else {
		nums := 0
		for _, v := range r.votes {
			if v {
				nums++
			}
		}

		if nums > quorum {
			r.becomeLeader()
		} else if curVoteLens-nums > quorum {
			r.becomeFollower(r.Term, None)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// case 1
	//for i, entry := range r.RaftLog.entries {
	//	log.Infof("node:%v, handleAppendEntries check r.RaftLog.entries, i:%v, Term:%v, Index:%v", r.id, i, entry.Term, entry.Index)
	//}

	//for i, entry := range m.Entries {
	//	log.Infof("node:%v, check m.Entries, i:%v, Term:%v, Index:%v", r.id, i, entry.Term, entry.Index)
	//}

	//log.Infof("node:%v, handleAppendEntries1, m.Term:%v, r.Term:%v", r.id, m.Term, r.Term)
	if m.Term != None && m.Term < r.Term {
		r.sendAppendEntriesResponse(m.From, true, None, None)
		return
	}

	if m.Term != None && m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	r.Lead = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// check log
	lastLogIndex := r.RaftLog.LastIndex()
	//log.Infof("node:%v, handleAppendEntries2, firstIndex:%v, lastLogIndex:%v, m.Index:%v", r.id, r.RaftLog.firstIndex, lastLogIndex, m.Index)

	// case 2:
	// first.......last
	// |..............|...index
	if lastLogIndex < m.Index {
		r.sendAppendEntriesResponse(m.From, true, lastLogIndex+1, None)
		return
	}

	// the append log in the node raftlog
	// case 3:
	// first..............last
	// |.......index......|
	if r.RaftLog.firstIndex <= m.Index {
		logTerm, err := r.RaftLog.Term(m.Index)
		//	log.Infof("node:%v, handleAppendEntries3, logTerm:%v, m.Term:%v", r.id, logTerm, m.Term)

		if err != nil {
			panic(err)
		}
		// term not match
		if logTerm != m.LogTerm {
			// sreach the match log by binary sreach
			//		log.Infof("node:%v, handleAppendEntries term not match, m.Index:%v, r.RaftLog.firstIndex:%v", r.id, m.Index, r.RaftLog.firstIndex)
			MatchLogIndex := sort.Search(
				int(m.Index-r.RaftLog.firstIndex+1),
				func(i int) bool {
					return r.RaftLog.entries[i].Term == logTerm
				})

			//		log.Infof("node:%v, handleAppendEntries, MatchLogIndex:%v, r.RaftLog.entries[MatchLogIndex].Term:%v, logTerm:%v",
			//			r.id, MatchLogIndex, r.RaftLog.entries[MatchLogIndex].Term, logTerm)
			r.sendAppendEntriesResponse(m.From, true, uint64(MatchLogIndex+int(r.RaftLog.firstIndex)), logTerm)
			return
		}
	}

	// term match
	// delete the entries from [index+1:last]
	for i, entry := range m.Entries {
		//	log.Infof("node:%v, handleAppendEntries4, entry.Index:%v, r.RaftLog.firstIndex:%v, lastLogIndex:%v",
		//		r.id, entry.Index, r.RaftLog.firstIndex, lastLogIndex)
		if entry.Index < r.RaftLog.firstIndex {
			continue
		}

		if entry.Index <= r.RaftLog.LastIndex() {
			logTerm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			//		log.Infof("node:%v, handleAppendEntries5, logTerm:%v, entry.Term:%v",
			//			r.id, logTerm, entry.Term)

			if logTerm != entry.Term {
				idx := entry.Index - r.RaftLog.firstIndex
				r.RaftLog.entries[idx] = *entry
				r.RaftLog.entries = r.RaftLog.entries[:idx+1]
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			}
		} else {
			// add the entries from [last+1:]
			size := len(m.Entries)
			for j := i; j < size; j++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
			}
		}
	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendEntriesResponse(m.From, false, r.RaftLog.LastIndex(), None)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, false)
		return
	}

	r.Lead = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendHeartbeatResponse(m.From, true)
}

// the client propose log entries to the raft
// finally, the propose will warped as this message
func (r *Raft) handlePropose(m pb.Message) {
	lastLogIndex := r.RaftLog.LastIndex()
	entries := m.Entries

	for i, entry := range entries {
		entry.Term = r.Term
		entry.Index = lastLogIndex + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	} else {
		r.broadcastAppend()
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	// get the information in the snapshot and apply state
	//
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	meta := m.Snapshot.Metadata

	// the node log entries newer than snapshot
	if meta.Index <= r.RaftLog.committed {
		r.sendAppendEntriesResponse(m.From, false, r.RaftLog.committed, None)
		return
	}

	// update state
	r.RaftLog.firstIndex = meta.Index + 1
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.RaftLog.entries = nil

	r.Prs = map[uint64]*Progress{}
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}

	r.RaftLog.pendingSnapshot = m.Snapshot
	r.sendAppendEntriesResponse(m.From, false, r.RaftLog.LastIndex(), None)
}

func (r *Raft) handleBeat(m pb.Message) {
	for peer := range r.Prs {
		// log.Infof("node:%v, handleBeat, sendHeartbeat, to:%v", r.id, peer)
		if peer == r.id {
			continue
		} else {
			r.sendHeartbeat(peer)
		}
	}
}

// handleAppendEntriesResponse handle AppendEntriesResponse RPC request
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	// log.Infof("node:%v, handleAppendEntriesResponse, m.Reject:%v", r.id, m.Reject)
	if m.Reject {
		// append entries was rejected

		// case 1
		if m.Index == None {
			return
		}

		// case 2
		if m.LogTerm == None {
			r.Prs[m.From].Next = m.Index
			r.sendAppend(m.From)
			return
		} else {
			// case 3
			//log.Infof("node:%v, handleAppendEntriesResponse case 3, m.Index:%v", r.id, m.Index)
			logTerm := m.LogTerm
			SearchIndex := sort.Search(len(r.RaftLog.entries),
				func(i int) bool { return r.RaftLog.entries[i].Term > logTerm })
			//log.Infof("node:%v, handleAppendEntriesResponse case 3, SearchIndex:%v", r.id, SearchIndex)

			index := m.Index
			//log.Infof("node:%v, handleAppendEntriesResponse case 3, r.RaftLog.entries[SearchIndex-1].Term:%v, logTerm:%v",
			//	r.id, r.RaftLog.entries[SearchIndex-1].Term, logTerm)
			if SearchIndex > 0 && r.RaftLog.entries[SearchIndex].Term == logTerm {
				index = uint64(SearchIndex) + r.RaftLog.firstIndex
			}
			//log.Infof("node:%v, handleAppendEntriesResponse case 3, index:%v", r.id, index)
			r.Prs[m.From].Next = index
			r.sendAppend(m.From)
			return
		}
	} else {
		//log.Infof("node:%v, handleAppendEntriesResponse, m.Index:%v, r.Prs[m.From].Match:%v", r.id, m.Index, r.Prs[m.From].Match)
		if m.Index > r.Prs[m.From].Match {
			r.Prs[m.From].Next = m.Index + 1
			r.Prs[m.From].Match = m.Index
			r.Commit()
		}
	}
}
