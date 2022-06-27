package raft

import (
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) handleElection() {
	r.becomeCandidate()

	r.heartbeatElapsed = 0
	// r.electionElapsed = r.electionTimeout + rand.Intn(r.electionTimeout)

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

	lastLogIndex := r.RaftLog.lastIndex
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		panic(err)
	}

	if lastLogTerm > m.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if lastLogTerm == m.Term && lastLogIndex > m.Index {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	// accpet!
	r.Vote = m.From
	r.electionElapsed = 0
	r.sendRequestVoteResponse(m.From, false)

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None && m.Term < r.Term {
		r.sendAppendEntriesResponse(m.From, true, None, None)
		return
	}

	if m.Term != None && m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	r.electionElapsed = 0

	// check log
	lastLogIndex := r.RaftLog.LastIndex()

	// stuation 1:
	// first.......last
	// |..............|...index
	if lastLogIndex < m.Index {
		r.sendAppendEntriesResponse(m.From, true, lastLogIndex+1, None)
		return
	}

	// the append log in the node raftlog
	// stuation 2:
	// first..............last
	// |.......index......|
	if r.RaftLog.firstIndex <= m.Index {
		logTerm, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic(err)
		}
		// term not match
		if logTerm != m.LogTerm {
			// sreach the match log by binary sreach
			MatchLogIndex := sort.Search(
				int(m.Index-r.RaftLog.firstIndex+1),
				func(i int) bool {
					return r.RaftLog.entries[i].Term == logTerm
				})
			r.sendAppendEntriesResponse(m.From, true, uint64(MatchLogIndex+int(r.RaftLog.firstIndex)), logTerm)
			return
		} else {
			// term match
			// delete the entries from [index+1:last]
			for i, entry := range m.Entries {
				if entry.Index < r.RaftLog.firstIndex {
					continue
				}

				if entry.Index <= lastLogIndex {
					logTerm, err = r.RaftLog.Term(entry.Index)
					if err != nil {
						panic(err)
					}
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
		}
	}

	r.sendAppendEntriesResponse(m.From, false, r.RaftLog.LastIndex(), None)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, false)
	}
}

func (r *Raft) handlePropose(m pb.Message) {

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}
