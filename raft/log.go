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
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/mylog"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	peerid uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hard, _, err := storage.InitialState()
	if err != nil {
		panic(" storage error ")
	}

	ents := make([]pb.Entry, 1)
	first, _ := storage.FirstIndex()
	last, _ := storage.LastIndex()
	oldinx := first - 1
	oldterm, err := storage.Term(oldinx)
	if err != nil {
		panic(fmt.Sprintf(" get old inx  %d term error %v", oldinx, err))
	}
	ents = []pb.Entry{{Index: oldinx, Term: oldterm}}

	logs, err := storage.Entries(first, last+1)
	if err != nil {
		panic(fmt.Sprintf(" get storage entries from %d to %d  error %v", first, last+1, err))
	}
	ents = append(ents, logs...)
	if hard.Commit > ents[len(ents)-1].Index {
		panic(fmt.Sprintf(" hard.Commit %d is bigger than storage's lastlog index %d ", hard.Commit, ents[len(ents)-1].Index))
	}
	raftlog := &RaftLog{
		entries:   ents,
		storage:   storage,
		stabled:   last,
		committed: hard.Commit,
		//applied:   0,
		applied: oldinx,
		//peerid:    peerid,
	}
	return raftlog
}
func (l *RaftLog) setpeerid(peerid uint64) {
	l.peerid = peerid
}

func (l *RaftLog) append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	firstlog := l.entries[0].Index + 1
	lastlog := l.lastIndex()

	last := entries[0].Index + uint64(len(entries)) - 1
	first := entries[0].Index

	// _________ents____          ___ents_______
	//                    ****  log
	if last < firstlog || lastlog+1 < first {
		return errors.NotValidf("")
	}

	// truncate compacted entries 已经snapshot了
	//  2 3 4 5 6 7 8 9 ents               5  6 7 8 9
	//     4  5  6 7  log  firstlog  5     4  5  6  7
	if firstlog > entries[0].Index {
		entries = entries[firstlog-entries[0].Index:]
	}
	badindex, badterm := uint64(0), uint64(0)
	offset := entries[0].Index - l.entries[0].Index
	switch {
	case uint64(len(l.entries)) > offset:
		inx := 0
		for inx < len(entries) && int(offset) < len(l.entries) {
			if entries[inx].Index == l.entries[offset].Index && entries[inx].Term == l.entries[offset].Term {
				offset++
				inx++
			} else {
				badindex = entries[inx].Index
				badterm = entries[inx].Term
				mylog.Printf(mylog.LevelAppendEntry, "peer %d in append index and term not equal index %d : %d term %d : %d ",
					l.peerid, entries[inx].Index, l.entries[offset].Index, entries[inx].Term, l.entries[offset].Term)
				break
			}
		}
		// inx = 3  offset 4
		oldentries := l.entries
		l.entries = append([]pb.Entry{}, l.entries[:offset]...)
		laststable := l.stabled
		l.stabled = min(l.lastIndex(), laststable) // 有些数据不准确，被截断了， 先把原来正确的拷贝过来，更新stabled
		if inx == len(entries) {                   // 1,1    1,1  2,2
			l.entries = append(l.entries, oldentries[offset:]...)
			// 没有新数据了，那么剩余的老数据依然准确，拷贝过来，更新stabled 但是不能比原来的数值大
			l.stabled = min(l.lastIndex(), laststable)
		} else {
			l.entries = append(l.entries, entries[inx:]...)
		}
		if badindex != 0 && badterm != 0 {
			for _, item := range l.entries {
				if item.Index == badindex {
					mylog.Printf(mylog.LevelAppendEntry, "peer %d after different badindex %d badterm %d  to  term %d ",
						l.peerid, badindex, badterm, item.Term)
					break
				}
			}
		}
		if l.stabled != laststable {
			mylog.Printf(mylog.LevelAppendEntry, "peer %d stableed changed from %d to %d ", l.peerid, laststable, l.stabled)
		}

	case uint64(len(l.entries)) == offset:
		l.entries = append(l.entries, entries...)
	default:
		log.Panicf("missing log entry [last: %d, append at: %d]",
			l.lastIndex(), entries[0].Index)
	}
	return nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) MaybeCompact(compactinx uint64, compactterm uint64) (error, uint64, uint64) {
	// Your Code Here (2C).

	if l.applied < compactinx {
		compactinx = l.applied - 1
		var err error = nil
		compactterm, err = l.Term(compactinx)
		if err != nil {
			return err, 0, 0
		}
	}

	if compactinx <= l.entries[0].Index || compactinx > l.applied {
		return errors.New(fmt.Sprintf("peer %d bad compact index", l.peerid)), 0, 0
	}
	mylog.Printf(mylog.LevelCompactSnapshot, "peer %d  maybecompact index: %d  term : %d  applyied is %d ", l.peerid, compactinx, compactterm, l.applied)

	bfind := false
	inx := 0
	for _inx, item := range l.entries {
		if compactinx == item.Index && compactterm == item.Term {
			bfind = true
			inx = _inx
		}
	}
	if bfind == false {
		panic(" this can not happen, when compact")
	}
	ents := []pb.Entry{{Index: compactinx, Term: compactterm}}
	ents = append(ents, l.entries[inx+1:]...)
	l.entries = ents
	return nil, compactinx, compactterm
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	dst, _ := l.getEntries(l.firstIndex(), l.lastIndex()+1)
	return dst
}

func (l *RaftLog) firstIndex() uint64 {
	return l.entries[0].Index + 1
}

func (l *RaftLog) lastIndex() uint64 {
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}
func (l *RaftLog) LastIndexTerm() (uint64, uint64) {
	return l.entries[len(l.entries)-1].Index, l.entries[len(l.entries)-1].Term
}

// [lo,hi).
func (l *RaftLog) getEntries(lo, hi uint64) ([]pb.Entry, error) {
	offset := l.entries[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, l.lastIndex())
	}

	ents := l.entries[lo-offset : hi-offset]
	if len(l.entries) == 1 && len(ents) != 0 {
		// only contains dummy entries.
		return nil, ErrUnavailable
	}
	return ents, nil
}
func (l *RaftLog) filterentries(ents []pb.Entry) []pb.Entry {
	if ents == nil {
		return nil
	}
	var dst []pb.Entry
	for _, item := range l.entries {
		if item.Data != nil {
			dst = append(dst, item)
		}
	}
	return dst
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.LastIndex() > l.stabled {
		dst, _ := l.getEntries(l.stabled+1, l.LastIndex()+1)
		return dst
	}
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.committed > l.applied {
		dst, _ := l.getEntries(l.applied+1, l.committed+1)
		return dst
	}
	return nil
}

func (l *RaftLog) getPendiingConfIndex() uint64 {
	for _, item := range l.entries {
		if item.Index > l.applied && item.EntryType == pb.EntryType_EntryConfChange {
			return item.Index
		}
	}
	return 0
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.lastIndex()
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	offset := l.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[i-offset].Term, nil
}

func (l *RaftLog) getLastValidIndexTerm() (uint64, uint64) {
	return l.entries[len(l.entries)-1].Index, l.entries[len(l.entries)-1].Term
}

// return perv inx, term
func (l *RaftLog) getOlderNextIndexfromTerm(term uint64) (uint64, uint64) {
	previnx, prevterm := uint64(0), uint64(0)
	for inx := len(l.entries) - 1; inx >= 0; inx-- {
		if prevterm == 0 && l.entries[inx].Term < term {
			prevterm = l.entries[inx].Term
			previnx = l.entries[inx].Index
			break
		}
	}
	if previnx == l.entries[0].Index {
		return l.entries[1].Index, l.entries[1].Term
	}
	return previnx, prevterm
}
func (l *RaftLog) Info() string {
	return fmt.Sprintf(" raftlog: len %d stabled %d commited %d apply %d  first: inx %d term %d -- last: inx %d term %d ",
		len(l.entries), l.stabled, l.committed, l.applied, l.entries[0].Index, l.entries[0].Term, l.entries[len(l.entries)-1].Index, l.entries[len(l.entries)-1].Term)
}
