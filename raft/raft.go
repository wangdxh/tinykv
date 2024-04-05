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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
	"time"
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

var ErrNotLeader = errors.New("i am not leader")

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
	Commited    uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
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

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	Peers        []uint64
	StateChanged bool
	EleTimeout   int
	DebugLevel   int
	PrintfPrefix string
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hard, _, err := c.Storage.InitialState()
	if err != nil {
		panic(" what the fuck")
	}
	r := &Raft{}
	r.id = c.ID
	r.Term = hard.GetTerm()
	r.Vote = hard.GetVote()
	r.EleTimeout = c.ElectionTick
	r.electionTimeout = r.EleTimeout
	r.heartbeatTimeout = c.HeartbeatTick

	for _, item := range c.peers {
		if item != r.id {
			r.Peers = append(r.Peers, item)
		}
	}
	r.Peers = append(r.Peers, r.id)

	r.RaftLog = newLog(c.Storage)
	r.RaftLog.applied = c.Applied
	r.becomeFollower(r.Term, None)
	return r
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			/*r.electionElapsed = 0*/
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if r.Term > term {
		panic(" can not happend ")
	}

	if term > r.Term || r.State != StateFollower {
		r.Vote = None
		r.StateChanged = true

		r.State = StateFollower
		r.Term = term
		r.electionTimeout = r.EleTimeout + rand.Intn(r.EleTimeout)
	}
	//r.Term = term
	//r.State = StateFollower
	r.electionElapsed = 0
	r.Lead = lead

}
func (r *Raft) onlyme() bool {
	return len(r.Peers) == 1 && r.Peers[0] == r.id
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.electionElapsed = 0
	r.electionTimeout = r.EleTimeout + rand.Intn(r.EleTimeout)
	r.StateChanged = true

	r.Term += 1
	r.Lead = None
	r.Vote = r.id
	r.votes = make(map[uint64]bool, len(r.Peers))
	r.votes[r.id] = true

	if r.onlyme() {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Printf(0, " i become leader ")
	r.State = StateLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.StateChanged = true

	r.Lead = r.id
	r.Prs = make(map[uint64]*Progress)

	for _, item := range r.Peers {
		if item != r.id {
			r.Prs[item] = &Progress{
				Match: 0,
				Next:  r.RaftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[item] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}
	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{Data: nil}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.MsgType == pb.MessageType_MsgHup {
		if r.State != StateLeader {
			r.becomeCandidate()
			if false == r.onlyme() {
				// 避免重复发送
				for inx := len(r.msgs) - 1; inx >= 0; inx-- {
					if r.msgs[inx].MsgType == pb.MessageType_MsgRequestVote {
						r.msgs = append(r.msgs[:inx], r.msgs[inx+1:]...)
					}
				}
				for _, val := range r.Peers {
					if val != r.id {
						r.sendRequestVote(val)
					}
				}
			}
		}
		return nil
	} else if m.MsgType == pb.MessageType_MsgBeat {
		if r.State == StateLeader {
			for _, peer := range r.Peers {
				if peer != r.id {
					_ = r.sendHeartbeat(peer)
				}
			}
		}
		return nil
	} else if m.MsgType == pb.MessageType_MsgPropose {
		if r.State == StateLeader {
			var ents []pb.Entry
			startinx := r.RaftLog.LastIndex()
			for i, item := range m.Entries {
				ents = append(ents, pb.Entry{
					EntryType: item.EntryType,
					Term:      r.Term,
					Index:     startinx + uint64(1+i),
					Data:      item.Data,
				})
			}
			_ = r.RaftLog.append(ents)
			val := r.Prs[r.id]
			val.Match = r.RaftLog.LastIndex()
			val.Next = val.Match + 1
			r.Prs[r.id] = val

			if r.onlyme() {
				r.RaftLog.committed = r.RaftLog.lastIndex()
			} else {
				for _, peer := range r.Peers {
					if peer != r.id {
						r.sendAppend(peer)
					}
				}
			}

		} else {
			return ErrNotLeader
		}
	}

	if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
		if r.State == StateLeader && m.Term == r.Term {
			panic(" big problome, election error , now have  two leader !!!!!!!!!!!!!!!!!")
		}

		if m.Term > r.Term ||
			(m.Term == r.Term &&
				(r.State != StateFollower || r.Lead != m.From)) {
			r.becomeFollower(m.Term, m.From)
		}
	} else if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	// m.Term 小于 r.term 的情况，都是reject = true, 然后response
	if r.State == StateFollower {
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			{
				return r.handleRequestVote(m)
			}
		case pb.MessageType_MsgAppend:
			{
				return r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgHeartbeat:
			{
				return r.handleHeartbeat(m)
			}
		}
	} else if r.State == StateLeader {
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeatResponse:
			{
				return r.handleHeartbeatResponse(m)
			}
		case pb.MessageType_MsgAppendResponse:
			{
				return r.handleAppendEntriesResponse(m)
			}
		case pb.MessageType_MsgRequestVote:
			{
				r.msgs = append(r.msgs, r.getResponse(m, pb.MessageType_MsgRequestVoteResponse))
				return nil
			}
		}
	} else if r.State == StateCandidate {
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse && m.Term == r.Term {
			return r.handleRequestVoteResponse(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			return r.handleRequestVote(m)
		}

	}
	return nil
}
func (r *Raft) getResponse(m pb.Message, rspType pb.MessageType) pb.Message {
	response := pb.Message{
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		MsgType: rspType,
		Reject:  true,
	}
	return response
}
func (r *Raft) getAppendRequest(to uint64, preinx uint64, preterm uint64) pb.Message {
	return pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Commit:  r.RaftLog.committed,
		Index:   preinx,
		LogTerm: preterm,
		Entries: nil,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if to == r.id {
		panic(" stupid")
	}
	if r.State == StateLeader {
		progress := r.Prs[to]
		if r.RaftLog.LastIndex() >= progress.Next {
			if progress.Next < 1 {
				progress.Next = 1
			}
			preinx := progress.Next - 1
			preterm, err1 := r.RaftLog.Term(preinx)
			ents, err2 := r.RaftLog.getEntries(progress.Next, min(progress.Next+1000, r.RaftLog.LastIndex()+1))
			if err1 == ErrCompacted || err2 == ErrCompacted {
				panic(" not implenmented")
			} else if err1 == nil && err2 == nil && len(ents) > 0 {
				req := r.getAppendRequest(to, preinx, preterm)
				for inx := 0; inx <= len(ents)-1; inx++ {
					req.Entries = append(req.Entries, &ents[inx])
				}
				val := r.Prs[to]
				val.Commited = r.RaftLog.committed
				r.Prs[to] = val

				r.Printf(0, " send append to %d  :  Index : %d , LogTerm : %d commit %d ents : %d ", to, req.Index, req.LogTerm, req.Commit, len(req.Entries))
				r.msgs = append(r.msgs, req)
			}
		} else if progress.Match == r.RaftLog.LastIndex() {
			preinx, preterm := r.RaftLog.LastIndexTerm()
			req := r.getAppendRequest(to, preinx, preterm)
			val := r.Prs[to]
			val.Commited = r.RaftLog.committed
			r.Prs[to] = val

			r.Printf(0, " send append to %d  :  Index : %d , LogTerm : %d commit %d ents : %d ", to, req.Index, req.LogTerm, req.Commit, len(req.Entries))
			r.msgs = append(r.msgs, req)
		}
		return true
	}
	return false
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) error {
	// Your Code Here (2A).
	resp := r.getResponse(m, pb.MessageType_MsgAppendResponse)
	resp.Index = m.Index
	resp.LogTerm = m.LogTerm
	if m.Term >= r.Term {
		r.electionElapsed = 0
		logterm, err := r.RaftLog.Term(m.Index)
		if err != nil || m.LogTerm != logterm {
			r.Printf(0, " handle append reject because err %v  mlogtem %d  logterm %d ", err, m.LogTerm, logterm)
			goto OUT
		}

		if len(m.Entries) > 0 {
			if m.Index+1 != m.Entries[0].Index {
				panic(" index error ")
			}
			var ents []pb.Entry
			for _, item := range m.Entries {
				ents = append(ents, *item)
			}
			err = r.RaftLog.append(ents)
			if err != nil {
				goto OUT
			}
		}
		// 防不胜防啊，没有特殊的测试用例，这代码谁能写的对
		m.Commit = min(m.Commit, m.Index+uint64(len(m.Entries)))
		lastindex := r.RaftLog.LastIndex()
		lastterm, _ := r.RaftLog.Term(lastindex)
		if lastterm == r.Term && m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex()) // lastindex)
			r.Printf(0, "follower update commited to %d ", r.RaftLog.committed)
		}
		resp.Reject = false
		resp.Index = m.Index + uint64(len(m.Entries))
	}
OUT:
	r.msgs = append(r.msgs, resp)
	return nil
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) error {
	// Your Code Here (2A).
	r.Printf(0, " get append response from %d  reject %v m.index %d  ", m.From, m.Reject, m.Index)
	val := r.Prs[m.From]
	if m.Reject == true {
		inx, _ := r.RaftLog.getPreviousIndexTerm(m.LogTerm)
		val.Match = 0
		val.Next = inx
		val.Commited = 0
		r.Prs[m.From] = val
		r.sendAppend(m.From)
		return nil
	} else {
		val.Match = m.Index
		val.Next = val.Match + 1
		r.Prs[m.From] = val
	}

	var newcommit uint64 = 0
	//if true
	{
		var checkcommit []uint64
		for _, pid := range r.Peers {
			checkcommit = append(checkcommit, r.Prs[pid].Match)
		}
		sort.Slice(checkcommit, func(i, j int) bool { return checkcommit[i] > checkcommit[j] })

		// 5，3，4   5,4,3  3/2+1-1 = 1
		// 3, 5   5,3   2/2+1-1 = 1
		quorum := len(r.Peers)/2 + 1 - 1
		newcommit = checkcommit[quorum]
		r.Printf(0, "new commit %d in  %v", newcommit, checkcommit)
	}

	if newcommit > r.RaftLog.committed {
		term, _ := r.RaftLog.Term(newcommit)
		if term == r.Term {
			r.RaftLog.committed = newcommit
			r.Printf(0, "leader update commited to %d ", newcommit)
		}
	}
	// broadcast commtied， 在commited 更改和之前之后，response的peer 可能不会收到commited更新的消息，所以要遍历
	for key, val := range r.Prs {
		if key != r.id {
			if val.Match == r.RaftLog.lastIndex() && val.Commited < r.RaftLog.committed {
				r.Printf(0, " test  peer %d  valmatch %d r.lastindex %d valcommited %d  r.commited %d",
					key, val.Match, r.RaftLog.lastIndex(), val.Commited, r.RaftLog.committed)
				r.sendAppend(key)
			}
		}
	}

	return nil
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) error {
	// Your Code Here (2A).
	req := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	}
	r.msgs = append(r.msgs, req)
	return nil
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) error {
	// Your Code Here (2A).
	response := r.getResponse(m, pb.MessageType_MsgHeartbeatResponse)
	if m.Term >= r.Term {
		r.electionElapsed = 0
		response.Index = r.RaftLog.LastIndex()
		response.Reject = false
	}
	r.msgs = append(r.msgs, response)
	return nil
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term < r.Term {
		return nil
	}

	if m.Reject == false {
		//val := r.Prs[m.From]
		if m.Index < r.RaftLog.LastIndex() {
			r.Printf(0, " get heartbeat resp from %d reject %v  r.lastindex %d m.index %d",
				m.From, m.Reject, r.RaftLog.lastIndex(), m.Index)
			r.sendAppend(m.From)
		}
	}
	return nil
}

func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	inx, term := r.RaftLog.getLastValidIndexTerm()
	req := pb.Message{
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgRequestVote,
		Term:    r.Term,
		LogTerm: term,
		Index:   inx,
	}
	r.Printf(0, " send request vote to %d   index %d logterm %d ", to, inx, term)
	r.msgs = append(r.msgs, req)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleRequestVote(m pb.Message) error {
	// Your Code Here (2A).
	lastindex, lastterm := r.RaftLog.getLastValidIndexTerm()
	resp := r.getResponse(m, pb.MessageType_MsgRequestVoteResponse)

	if m.Term >= r.Term && (r.Vote == None || r.Vote == m.From) &&
		(m.LogTerm > lastterm || (m.LogTerm == lastterm && m.Index >= lastindex)) {
		resp.Reject = false
		r.Vote = m.From
	}
	//Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	//If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	//If the logs end with the same term, then whichever log is longer is more up-to-date.
	r.Printf(0, " handle requestvote m.term %d  m.from %d  myvote %d mystate %s  reject %v  %d %d %d %d",
		m.Term, m.From, r.Vote, r.State.String(), resp.Reject,
		m.LogTerm, lastterm, m.Index, lastindex)

	r.msgs = append(r.msgs, resp)
	return nil
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term < r.Term {
		return nil
	}
	if r.State == StateCandidate {
		r.Printf(0, " get requestvote resp from %d  reject: %v m.term: %d", m.From, m.Reject, m.Term)
		r.votes[m.From] = !m.Reject
		nums := 0
		for _, val := range r.votes {
			if val == true {
				nums++
			}
		}
		if nums >= len(r.Peers)/2+1 {
			r.becomeLeader()
		} else if len(r.votes) == len(r.Peers) {
			// 没有达成，返回为follower,
			r.becomeFollower(r.Term, None)
		}
	}
	return nil
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) Ready() Ready {
	// Your Code Here (2A).
	hard := pb.HardState{}
	if r.StateChanged {
		hard.Vote = r.Vote
		hard.Commit = r.RaftLog.committed
		hard.Term = r.Term
	}
	return Ready{
		HardState:        hard,
		Entries:          r.RaftLog.unstableEntries(),
		Messages:         r.msgs[:],
		CommittedEntries: r.RaftLog.nextEnts(),
	}
}

// HasReady called when RawNode user need to check if any Ready pending.
func (r *Raft) HasReady() bool {
	// Your Code Here (2A).
	return len(r.msgs) > 0 || len(r.RaftLog.nextEnts()) > 0 || len(r.RaftLog.unstableEntries()) > 0 || r.StateChanged
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (r *Raft) Advance(rd Ready) {
	// Your Code Here (2A).
	if len(rd.Messages) > 0 {
		r.msgs = r.msgs[len(rd.Messages):]
	}
	if len(rd.CommittedEntries) > 0 {
		r.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	if len(rd.Entries) > 0 {
		r.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}

	if r.StateChanged &&
		rd.HardState.Term == r.Term &&
		rd.HardState.Vote == r.Vote &&
		rd.HardState.Commit == r.RaftLog.committed {
		r.StateChanged = false
	}
}

func (r *Raft) Printf(level int, format string, a ...interface{}) {
	if r.DebugLevel >= level {
		str := fmt.Sprintf(format, a...)
		format := "2006-01-02 15:04:05.000"
		fmt.Printf("%s%d term %d %s -- %s\n", r.PrintfPrefix, r.id, r.Term, time.Now().Format(format), str)
	}
}
