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
	"github.com/pingcap-incubator/tinykv/mylog"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"math/rand"
	"sort"
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

func (c *Config) Setpeers(peers []uint64) {
	c.peers = peers
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
	Sending     bool
	SendTick    uint64
	//SingleSendround uint64
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

	Peers            []uint64
	StateChanged     bool
	EleTimeout       int
	DebugLevel       int
	PrintfPrefix     string
	Snapshot         *pb.Snapshot
	monoticks        uint64 // when in test cases ，monoticks maybe is 0
	candidateTimeout uint64
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
	//r.electionTimeout = r.EleTimeout
	r.electionTimeout = r.EleTimeout + rand.Intn(r.EleTimeout)
	r.heartbeatTimeout = c.HeartbeatTick

	for _, item := range c.peers {
		{
			r.Peers = append(r.Peers, item)
		}
	}
	if len(r.Peers) == 0 {
		mylog.Printf(mylog.LevelBaisc, "test")
	}
	r.RaftLog = newLog(c.Storage)
	r.RaftLog.applied = max(c.Applied, r.RaftLog.applied)
	r.RaftLog.peerid = r.id
	r.initPrs()
	mylog.Printf(mylog.LevelBaisc, "peer %d newraft peers %v entries first %d-%d logs len : %d",
		r.id, r.Peers, r.RaftLog.entries[0].Index, r.RaftLog.entries[0].Term, len(r.RaftLog.entries))

	r.becomeFollower(r.Term, None)
	return r
}
func (r *Raft) initPrs() {
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
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.monoticks++
	if r.State == StateLeader {
		// 100 ms one tick
		// mylog.Printf(mylog.LevelBaisc, "leader  ticks   %d  %d ", r.heartbeatTimeout, r.electionTimeout)

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
	r.leadTransferee = 0
	r.PendingConfIndex = 0
	r.Lead = lead
	mylog.Printf(mylog.LevelBaisc, "peer %d term %d becomefollow  l %d ", r.id, r.Term, r.Lead)
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
	r.leadTransferee = 0
	r.PendingConfIndex = 0

	r.Term += 1
	r.Lead = None
	r.Vote = r.id
	r.votes = make(map[uint64]bool, len(r.Peers))
	r.votes[r.id] = true
	mylog.Printf(mylog.LevelBaisc, "peer %d term %d becomeCandidate  ", r.id, r.Term)

	if r.onlyme() {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.StateChanged = true
	r.leadTransferee = 0

	r.Lead = r.id
	r.initPrs()

	mylog.Printf(mylog.LevelBaisc, "peer %d term %d becomeleader", r.id, r.Term)
	r.PendingConfIndex = r.RaftLog.getPendiingConfIndex()

	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{Data: nil}},
	})
}

func (r *Raft) IsInGroup(id uint64) bool {
	_, has := r.Prs[id]
	return has
}

func (r *Raft) IsConfChangeEntry(entry *pb.Entry) bool {
	if entry.EntryType == pb.EntryType_EntryConfChange {
		return true
	} else if entry.EntryType == pb.EntryType_EntryNormal && entry.Data != nil {
		msg := new(raft_cmdpb.RaftCmdRequest)
		err := msg.Unmarshal(entry.Data)
		if err != nil {
			return false
		}
		return msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer
	}
	return false
}

func (r *Raft) IsDeleteMe(entry *pb.Entry) bool {
	if entry.EntryType == pb.EntryType_EntryConfChange {
		confchange := new(pb.ConfChange)
		err := confchange.Unmarshal(entry.Data)
		if err != nil {
			panic(" bad conf change")
		}
		if confchange.ChangeType == pb.ConfChangeType_RemoveNode && confchange.NodeId == r.id && r.IsInGroup(r.id) {
			return true
		}
	} else if entry.EntryType == pb.EntryType_EntryNormal && entry.Data != nil {
		msg := new(raft_cmdpb.RaftCmdRequest)
		err := msg.Unmarshal(entry.Data)
		if err != nil {
			panic("bad raftcmdrequest")
		}

		if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer &&
			msg.AdminRequest.ChangePeer.ChangeType == pb.ConfChangeType_RemoveNode &&
			msg.AdminRequest.ChangePeer.Peer.Id == r.id && r.IsInGroup(r.id) {
			return true
		}
	} else {
		panic(" sb")
	}
	return false
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.MsgType == pb.MessageType_MsgHup {
		if r.State != StateLeader {
			if r.IsInGroup(r.id) == false {
				return nil
			}

			r.becomeCandidate()
			r.candidateSendRequestVote()
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
			if r.leadTransferee != 0 {
				return errors.New("i am transfering leader")
			}

			var ents []pb.Entry
			isdeleteme := false
			startinx := r.RaftLog.LastIndex()
			for i, item := range m.Entries {
				ents = append(ents, pb.Entry{
					EntryType: item.EntryType,
					Term:      r.Term,
					Index:     startinx + uint64(1+i),
					Data:      item.Data,
				})

				if r.IsConfChangeEntry(&ents[len(ents)-1]) {
					if r.RaftLog.applied < r.PendingConfIndex {
						// not applied retur false
						mylog.Printf(mylog.LevelBaisc, "peer %d term %d configchagne error apply %d < pendingconf %d ",
							r.id, r.Term, r.RaftLog.applied, r.PendingConfIndex)
						return errors.New("has Pendingconfindex ")
					} else {
						r.PendingConfIndex = ents[len(ents)-1].Index
					}
					if r.IsDeleteMe(&ents[len(ents)-1]) {
						isdeleteme = true
					}
				}
			}

			if isdeleteme && len(r.Prs) >= 2 {
				var transferee uint64 = 0
				match := uint64(0)
				for k, v := range r.Prs {
					if k != r.id && v.Match > match {
						transferee = k
						match = v.Match
					}
				}
				_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee, To: r.id})
				mylog.Printf(mylog.LevelTransferLeader, "peer %d term %d admin want remove me, start TransferLeader, from %d to %d  peers : %v ",
					r.id, r.Term, r.id, transferee, r.Peers)
				return errors.New(fmt.Sprintf(" admin want remove me, start TransferLeader "))
			}

			for _, item := range ents {
				if item.Data == nil {
					mylog.Printf(mylog.LevelProposal, "peer %d term %d leader propose index %d  data is nil ", r.id, r.Term, item.Index)
				} else if item.Data != nil && item.EntryType == pb.EntryType_EntryNormal {
					msg := new(raft_cmdpb.RaftCmdRequest)
					err := msg.Unmarshal(item.Data)
					if err != nil {
						continue
					}
					r.Printraftcmdrequest(item.Index, msg, true)
				}
			}

			_ = r.RaftLog.append(ents)
			val := r.Prs[r.id]
			val.Match = r.RaftLog.LastIndex()
			val.Next = val.Match + 1
			r.Prs[r.id] = val

			mylog.Printf(mylog.LevelAppendEntry, "peer %d term %d propose index %d ",
				r.id, r.Term, r.RaftLog.lastIndex())
			if r.onlyme() {
				r.RaftLog.committed = r.RaftLog.lastIndex()
			} else {
				{
					for _, peer := range r.Peers {
						if peer != r.id {
							r.sendAppend(peer)
						}
					}
				}
			}

		} else {
			return ErrNotLeader
		}
	}

	if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
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
		case pb.MessageType_MsgSnapshot:
			{
				r.handleSnapshot(m)
				return nil
			}
		case pb.MessageType_MsgTimeoutNow, pb.MessageType_MsgTransferLeader:
			{
				mylog.Printf(mylog.LevelTransferLeader, "peer %d term %d get MessageType_MsgTimeoutNow start election", r.id, r.Term)
				return r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
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
		case pb.MessageType_MsgTransferLeader: //MsgTransferLeader message is local message that not come from network
			{
				if _, has := r.Prs[m.From]; has {
					if m.From == r.id {
						r.leadTransferee = 0 // 转移到自己，终止操作
					} else {
						r.leadTransferee = m.From // 触发转移
						r.CheckleaderTransfer(m.From, true)
					}
				}
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

func (r *Raft) CheckleaderTransfer(from uint64, bsendappend bool) bool {
	if r.leadTransferee > 0 && r.leadTransferee == from {
		if r.Prs[r.leadTransferee].Match == r.RaftLog.LastIndex() {
			response := pb.Message{
				To:      r.leadTransferee,
				From:    r.id,
				Term:    r.Term,
				MsgType: pb.MessageType_MsgTimeoutNow,
			}
			r.msgs = append(r.msgs, response)
			// 这里不能将leadTransferee 清0， 否则leadTransferee不一定能当选
		} else if bsendappend {
			r.sendAppend(r.leadTransferee)
		}
		return true
	}
	return false
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if to == r.id {
		panic(" stupid")
	}
	if r.State != StateLeader {
		return false
	}

	progress := r.Prs[to]
	if progress.Sending == true && r.monoticks > 0 { // monoticks == 0 sending 失效，为了通过测试用例TestLeaderCommitEntry2AB
		berase := false
		for _inx, msg := range r.msgs {
			if msg.To == to &&
				(msg.MsgType == pb.MessageType_MsgAppend || msg.MsgType == pb.MessageType_MsgSnapshot) {
				mylog.Printf(mylog.LevelAppendEntry, "peer %d send append to %d erase msg preindex %d preterm %d ents: %d ",
					r.id, to, msg.Index, msg.LogTerm, len(msg.Entries))
				r.msgs = append(r.msgs[0:_inx], r.msgs[_inx:]...)
				berase = true
				break
			}
		}
		if berase == false {
			mylog.Printf(mylog.LevelAppendEntry, " peer %d donotsendappend to %d because sending is false", r.id, to)
			return false
		}
	}
	// 12   --  12
	if r.RaftLog.LastIndex() >= progress.Next {
		mylog.Printf(mylog.LevelAppendEntryDetail, "peer %d term %d sending %v", r.id, r.Term, progress.Sending)
		if progress.Next < 1 {
			progress.Next = 1
		}
		preinx := progress.Next - 1
		preterm, err1 := r.RaftLog.Term(preinx)
		ents, err2 := r.RaftLog.getEntries(progress.Next, min(progress.Next+1000, r.RaftLog.LastIndex()+1))
		if err1 != nil || err2 != nil {
			err := err1
			if err == nil {
				err = err2
			}
			mylog.Printf(mylog.LevelCompactSnapshot, "peer %d sendappend to %d will snapshot err %v, preinx %d ",
				r.id, to, err, preinx)

			snapmsg := r.getAppendRequest(to, 0, 0)
			snap, err := r.RaftLog.storage.Snapshot()
			if err != nil {
				mylog.Printf(mylog.LevelCompactSnapshot, "peer %d sendappend to %d start snapshot() return err: %v", r.id, to, err)
				return false
			}
			mylog.Printf(mylog.LevelCompactSnapshot, "peer %d sendappend to %d start snapshot() ok : %v", r.id, to, err)
			snapmsg.Snapshot = &snap
			snapmsg.MsgType = pb.MessageType_MsgSnapshot
			r.msgs = append(r.msgs, snapmsg)

			val := r.Prs[to]
			val.Match = snap.Metadata.Index
			val.Next = val.Match + 1
			r.Prs[to] = val
		} else if err1 == nil && err2 == nil && len(ents) > 0 {
			req := r.getAppendRequest(to, preinx, preterm)
			for inx := 0; inx <= len(ents)-1; inx++ {
				req.Entries = append(req.Entries, &ents[inx])
			}
			/*val := r.Prs[to]
			val.SingleSendround = r.RaftLog.committed
			r.Prs[to] = val*/

			if len(req.Entries) > 0 && req.Entries[0].Index != preinx+1 {
				panic(" this can not happend")
			}

			mylog.Printf(mylog.LevelAppendEntry, "peer %d term %d send append to %d : preIndex: %d , preLogTerm: %d commit %d ents : %d ",
				r.id, r.Term, to, req.Index, req.LogTerm, req.Commit, len(req.Entries))
			r.msgs = append(r.msgs, req)
		} else {
			return false
		}
	} else if progress.Match == r.RaftLog.LastIndex() {
		preinx, preterm := r.RaftLog.LastIndexTerm()
		req := r.getAppendRequest(to, preinx, preterm)
		/*val := r.Prs[to]
		val.SingleSendround = r.RaftLog.committed
		r.Prs[to] = val*/

		mylog.Printf(mylog.LevelAppendEntry, "peer %d term %d send append to %d : preIndex: %d , preLogTerm: %d commit %d ents2 : %d ",
			r.id, r.Term, to, req.Index, req.LogTerm, req.Commit, len(req.Entries))
		r.msgs = append(r.msgs, req)
	} else {
		return false
	}
	progress.Sending = true
	progress.SendTick = r.monoticks
	r.Prs[to] = progress
	return true
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
			mylog.Printf(mylog.LevelAppendEntry, " peer %d term %d handleAppendEntries reject because err %v mindex %d mlogtem %d  logterm %d ",
				r.id, r.Term, err, m.Index, m.LogTerm, logterm)
			goto OUT
		}

		if len(m.Entries) > 0 {
			if m.Index+1 != m.Entries[0].Index {
				panic(fmt.Sprintf(" index error %d %d is not right msg %v \n", m.Index, m.Entries[0].Index, m))
			}
			var ents []pb.Entry
			for _, item := range m.Entries {
				ents = append(ents, *item)
			}
			mylog.Printf(mylog.LevelAppendEntryDetail, "peer %d term %d follower  raftlog len %d  append ents len %d ,first: inx %d term %d end: inx %d term %d from peer %d leaderis %d",
				r.id, r.Term, len(r.RaftLog.entries), len(ents), ents[0].Index, ents[0].Term, ents[len(ents)-1].Index, ents[len(ents)-1].Term, m.From, r.Lead)

			err = r.RaftLog.append(ents)

			tempents := r.RaftLog.entries
			mylog.Printf(mylog.LevelAppendEntry, "peer %d term %d follower after append raftlog len %d  first: inx %d term %d end: inx %d term %d ",
				r.id, r.Term, len(tempents), tempents[0].Index, tempents[0].Term, tempents[len(tempents)-1].Index, tempents[len(tempents)-1].Term)
			if err != nil {
				goto OUT
			}
		}
		// 防不胜防啊，没有特殊的测试用例，这代码谁能写的对
		m.Commit = min(m.Commit, m.Index+uint64(len(m.Entries)))
		lastindex := r.RaftLog.LastIndex()
		lastterm, _ := r.RaftLog.Term(lastindex)
		if lastterm == r.Term && m.Commit > r.RaftLog.committed {
			mylog.Printf(mylog.LevelAppendEntry, "peer %d term %d follower update commited from %d to %d ", r.id, r.Term, r.RaftLog.committed, min(m.Commit, r.RaftLog.LastIndex()))
			r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex()) // lastindex)
		}
		resp.Reject = false
		resp.Index = m.Index + uint64(len(m.Entries))
	}
OUT:
	if resp.Reject == true {
		// resp Commit 保存的是，本地的最后一个Index，先直接定位
		resp.Commit, _ = r.RaftLog.LastIndexTerm()
	} else {
		// 成功之后，返回commited
		resp.Commit = r.RaftLog.committed
	}
	r.msgs = append(r.msgs, resp)
	return nil
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) error {
	// Your Code Here (2A).
	//r.Printf(0, " get append response from %d  reject %v m.index %d  ", m.From, m.Reject, m.Index)
	peermatchchange := false
	val := r.Prs[m.From]
	val.Sending = false
	if m.Reject == true {
		var inx uint64 = 0
		if m.Index > m.Commit {
			inx = m.Commit
		} else {
			inx, _ = r.RaftLog.getOlderNextIndexfromTerm(m.LogTerm)
		}
		val.Match = 0
		val.Next = inx
		val.Commited = 0
		r.Prs[m.From] = val
		mylog.Printf(mylog.LevelAppendEntry, "peer %d term %d send append to %d Append reject m.Index %d m.logterm %d , Next will be %d ",
			r.id, r.Term, m.From, m.Index, m.LogTerm, inx)
		r.sendAppend(m.From)
		return nil
	} else {
		if val.Match != m.Index || m.Commit != val.Commited {
			peermatchchange = true
		}
		val.Match = m.Index
		val.Next = val.Match + 1
		val.Commited = m.Commit
		r.Prs[m.From] = val
	}
	mylog.Printf(mylog.LevelAppendEntryDetail, "peer %d term %d send append to %d Append Ok  match %d next %d  lastlogindex %d -- commited %d raftlogcommited %d ",
		r.id, r.Term, m.From, val.Match, val.Next, r.RaftLog.LastIndex(), m.Commit, r.RaftLog.committed)

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
		/*r.Printf(0, "new commit %d in  %v", newcommit, checkcommit)*/
	}

	//commitedchagne := false
	if newcommit > r.RaftLog.committed {
		term, _ := r.RaftLog.Term(newcommit)
		if term == r.Term {
			r.RaftLog.committed = newcommit
		}
	}

	// broadcast commtied， 在commited 更改和之前之后，response的peer 可能不会收到commited更新的消息，所以要遍历

	for key, val := range r.Prs {
		if key != r.id {
			if ((key == m.From && peermatchchange) || key != m.From) &&
				((val.Match == r.RaftLog.lastIndex() && val.Commited < r.RaftLog.committed) ||
					val.Next <= r.RaftLog.LastIndex()) &&
				val.Sending == false {
				//if val.Next <= r.RaftLog.lastIndex() || val.Commited < r.RaftLog.committed {
				/*r.Printf(0, " test  peer %d  valmatch %d r.lastindex %d valcommited %d  r.commited %d",
				key, val.Match, r.RaftLog.lastIndex(), val.Commited, r.RaftLog.committed)*/
				r.sendAppend(key)
			}
		}
	}

	if r.leadTransferee > 0 && r.leadTransferee == m.From {
		r.CheckleaderTransfer(m.From, true)
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
		response.Commit = r.RaftLog.committed
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

	mylog.Printf(mylog.LevelHeartbeat, " peer %d handleHeartbeatResponse reject %v from %d  m.index %d  myraftlog lastindex %d m.commit %d  r.raftlog.commited %d ",
		r.id, m.Reject, m.From, m.Index, r.RaftLog.LastIndex(), m.Commit, r.RaftLog.committed)

	if m.Reject == false {
		//if  {
		{
			if val, has := r.Prs[m.From]; has {
				val.Commited = m.Commit
				r.Prs[m.From] = val
			}
		}

		if m.Commit < r.RaftLog.committed || m.Index < r.RaftLog.LastIndex() {
			/*r.Printf(0, " get heartbeat resp from %d reject %v  r.lastindex %d m.index %d",
			m.From, m.Reject, r.RaftLog.lastIndex(), m.Index)*/
			val, has := r.Prs[m.From]
			if has == false {
				panic(" this can not happen123")
			}
			var appenddelayticks uint64 = 1 // 50ms   1s
			if r.monoticks == 0 || val.SendTick+appenddelayticks < r.monoticks {
				val.Sending = false
				r.Prs[m.From] = val
				r.sendAppend(m.From)
			}
		}
		r.CheckleaderTransfer(m.From, false)
	} else {
		panic(" this can not happen 888 heartbeatresp")
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
	//r.Printf(0, " send request vote to %d   index %d logterm %d ", to, inx, term)
	r.msgs = append(r.msgs, req)
}

// request vote 多发送一次，是在实际测试中，发现网络unreliable,丢数据时leader选举太菜
// 如果压根没有对方返回vote的回应，也不会再发
func (r *Raft) candidateSendRequestVote() {
	if false == r.onlyme() && r.State == StateCandidate {
		for inx := 0; inx < len(r.msgs); {
			if r.msgs[inx].MsgType == pb.MessageType_MsgRequestVote {
				r.msgs = append(r.msgs[:inx], r.msgs[inx+1:]...)
			} else {
				inx++
			}
		}
		for _, val := range r.Peers {
			_, has := r.votes[val]
			if val != r.id && has == false { // 本次term内，已经发送过的，就不发送了
				r.sendRequestVote(val)
			}
		}
	}
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
	/*r.Printf(0, " handle requestvote m.term %d  m.from %d  myvote %d mystate %s  reject %v  %d %d %d %d",
	m.Term, m.From, r.Vote, r.State.String(), resp.Reject,
	m.LogTerm, lastterm, m.Index, lastindex)*/

	r.msgs = append(r.msgs, resp)
	return nil
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term < r.Term {
		return nil
	}
	if r.State == StateCandidate {
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
		} else {
			// 再重新发送一次candidate的请求，在网络丢包的时候，通过增加发送请求次数，从而保证质量
			// 不然选举成功的会受到网络影响，时间大幅度延长
			r.candidateSendRequestVote()
		}
	}
	return nil
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	//  restore the raft internal state like the term, commit index and membership information, etc, from the eraftpb.SnapshotMetadata in the message,
	if r.Snapshot != nil {
		mylog.Printf(mylog.LevelCompactSnapshot, "peer %d  handleSnapshot discard because last snapshot is not empty ", r.id)
		return
	}
	if m.Snapshot.Metadata.Index <= r.RaftLog.firstIndex()-1 {
		mylog.Printf(mylog.LevelCompactSnapshot, "peer %d  handleSnapshot discard because snapshotindex %d <= firstindex-1 %d",
			r.id, m.Snapshot.Metadata.Index, r.RaftLog.firstIndex()-1)
		return
	}

	r.Snapshot = m.Snapshot
	r.Term = max(r.Term, m.Snapshot.Metadata.Term)
	r.StateChanged = true

	bfind := false
	inx := -1
	for _inx, ent := range r.RaftLog.entries {
		if ent.Index == m.Snapshot.Metadata.Index && ent.Term == m.Snapshot.Metadata.Term {
			bfind = true
			inx = _inx
			break
		}
	}

	oldentries := r.RaftLog.entries
	r.RaftLog.entries = []pb.Entry{
		{
			Term:  m.Snapshot.Metadata.Term,
			Index: m.Snapshot.Metadata.Index,
		},
	}
	//这三个值的处理容易出错
	//applyied  必须重置为snapshot的index
	//stabled commited 取当前log 和 snapshot 的最大值,
	r.RaftLog.applied = m.Snapshot.Metadata.Index
	if bfind == false {
		r.RaftLog.committed = m.Snapshot.Metadata.Index
		r.RaftLog.stabled = m.Snapshot.Metadata.Index
	} else {
		r.RaftLog.entries = append(r.RaftLog.entries, oldentries[inx+1:]...)
		r.RaftLog.stabled = max(r.RaftLog.stabled, m.Snapshot.Metadata.Index)
		r.RaftLog.committed = max(r.RaftLog.committed, m.Snapshot.Metadata.Index)
	}
	oldentries = nil

	for _, item := range m.Snapshot.Metadata.ConfState.Nodes {
		bfind := false
		for _, peer := range r.Peers {
			if peer == item {
				bfind = true
			}
		}
		if bfind == false {
			r.Peers = append(r.Peers, item)
		}
	}

	for i := 0; i < len(r.Peers); {
		exist := false
		for _, item := range m.Snapshot.Metadata.ConfState.Nodes {
			if r.Peers[i] == item {
				exist = true
			}
		}
		if exist == false {
			r.Peers = append(r.Peers[:i], r.Peers[i+1:]...)
		} else {
			i++
		}
	}
	r.initPrs()
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	for _, peer := range r.Peers {
		if peer == id {
			return
		}
	}
	r.Peers = append(r.Peers, id)
	if _, has := r.Prs[id]; has == true {
		panic(fmt.Sprintf("peer %d Prs had has id %d", r.id, id))
	}
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex(), // 为什么不是 LastIndex+1?  如果+1， 没有新的proposal， 新node不会进行更新
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	for inx, peer := range r.Peers {
		if peer == id {
			r.Peers = append(r.Peers[:inx], r.Peers[inx+1:]...)
			delete(r.Prs, id)
			if id == r.id {
				r.becomeFollower(r.Term, 0)
			}
			break
		}
	}
	// 只有我一个，提交剩余的条目
	if r.onlyme() && r.State == StateLeader {
		r.RaftLog.committed = r.RaftLog.lastIndex()
	}
}

func (r *Raft) Ready() Ready {
	// Your Code Here (2A).
	hard := pb.HardState{}
	if r.StateChanged {
		hard.Vote = r.Vote
		hard.Commit = r.RaftLog.committed
		hard.Term = r.Term
	}
	rd := Ready{
		//Entries:          r.RaftLog.unstableEntries(),
		Messages: r.msgs[:],
		//CommittedEntries: r.RaftLog.nextEnts(),
	}
	// 主要为了过测试用例
	if r.Lead > 0 {
		rd.SoftState = &SoftState{
			Lead:      r.Lead,
			RaftState: r.State,
		}
	}

	if r.Snapshot != nil {
		rd.Snapshot = *r.Snapshot
		stabledterm, err := r.RaftLog.Term(r.RaftLog.stabled)
		if err != nil {
			panic(fmt.Sprintf(" stabled index %d can not get term ", r.RaftLog.stabled))
		}
		rd.SnapshoRaftStateIndex = r.RaftLog.stabled
		rd.SnapshoRaftStateTerm = stabledterm
	} else {
		rd.Entries = r.RaftLog.unstableEntries()
		rd.CommittedEntries = r.RaftLog.nextEnts()
		rd.HardState = hard //已经更新了的 hardstate，如果没有给ready，下次会继续ready; 查看advanced
		// commited 这个值 和 raftlog 有些关联，已经commited的log 一定要在raftlog中存在,否则会抛异常
	}
	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
func (r *Raft) HasReady() bool {
	// Your Code Here (2A).
	return len(r.msgs) > 0 || len(r.RaftLog.nextEnts()) > 0 || len(r.RaftLog.unstableEntries()) > 0 || r.StateChanged || r.Snapshot != nil
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
	if r.Snapshot != nil && rd.Snapshot.Metadata == nil {
		r.Snapshot = nil
		mylog.Printf(mylog.LevelCompactSnapshot, "peer %d  Advance clear snapshot ", r.id)
	}
}

func (r *Raft) Printraftcmdrequest(Index uint64, msg *raft_cmdpb.RaftCmdRequest, inraft bool) {
	entrytype := ""
	if len(msg.Requests) > 0 {
		switch msg.Requests[0].CmdType {
		case raft_cmdpb.CmdType_Put:
			{
				entrytype = fmt.Sprintf(" request put %s %s", string(msg.Requests[0].Put.Key), string(msg.Requests[0].Put.Value))
			}
		case raft_cmdpb.CmdType_Delete:
			{
				entrytype = fmt.Sprintf(" request delete %s", string(msg.Requests[0].Delete.Key))
			}
		case raft_cmdpb.CmdType_Snap:
			{
				entrytype = fmt.Sprintf(" request snap")
			}
		case raft_cmdpb.CmdType_Get:
			{
				entrytype = fmt.Sprintf(" request get %s", string(msg.Requests[0].Get.Key))
			}
		}
	} else if msg.AdminRequest != nil {
		entrytype = fmt.Sprintf("adminreqest type: %v", msg.AdminRequest.CmdType)
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			{
				entrytype = fmt.Sprintf("adminreqest transfer leader : %v ", msg.AdminRequest.TransferLeader)
			}
		case raft_cmdpb.AdminCmdType_ChangePeer:
			{
				entrytype = fmt.Sprintf("adminreqest change peer : %v ", msg.AdminRequest.ChangePeer)
			}

		case raft_cmdpb.AdminCmdType_CompactLog:
			{
				entrytype = fmt.Sprintf("adminreqest compactlog : %v ", msg.AdminRequest.CompactLog)
			}
		case raft_cmdpb.AdminCmdType_Split:
			{
				entrytype = fmt.Sprintf("adminreqest split : %v ", msg.AdminRequest.Split)
			}
		}
	}
	mylog.Printf(mylog.LevelProposal, "peer %d term %d leader propose index %d in raft %v  %s ", r.id, r.Term, Index, inraft, entrytype)
}
