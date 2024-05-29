package raftstore

import (
	"fmt"
	"github.com/gogo/protobuf/sortkeys"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/mylog"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) getraftinfo(snap bool) string {
	return fmt.Sprintf("peer %s after Ready snapshot %v raftstate: index %d term %d, applystate: index %d trucate inx %d term %d , %s",
		d.Tag, snap,
		d.peerStorage.raftState.LastIndex, d.peerStorage.raftState.LastTerm,
		d.peerStorage.applyState.AppliedIndex, d.peerStorage.applyState.TruncatedState.Index, d.peerStorage.applyState.TruncatedState.Term,
		d.RaftGroup.Raft.RaftLog.Info())
}

func (d *peerMsgHandler) findpeerinraftpeers(raftpeerid uint64) (bool, int) {
	for inx, item := range d.RaftGroup.Raft.Peers {
		if item == raftpeerid {
			return true, inx
		}
	}
	return false, -1
}

func (d *peerMsgHandler) findpeerinregionpeers(raftpeerid uint64) (bool, int) {
	for inx, item := range d.Region().Peers {
		//mylog.Printf(mylog.LevelBaisc, " test %v %v  %v ", inx, item, d.Region().Peers)
		if item.Id == raftpeerid {
			return true, inx
		}
	}
	return false, -1
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}

	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()

		if ready.HardState.Term != 0 || len(ready.Entries) != 0 || len(ready.CommittedEntries) != 0 || ready.Snapshot.Metadata != nil {
			applystatewrite := false
			KvBatch := engine_util.WriteBatch{}

			if ready.Snapshot.Metadata != nil {
				if d.peerStorage.IsSnapShotApplying() && d.peerStorage.SnapShotApplyCheckOver() {
					ready.Snapshot.Metadata = nil
					//mylog.Printf(mylog.LevelCompactSnapshot, "peer %s Applysnapshot over ", d.Tag)
				} else {
					applystatewrite = true
					// 这段代码 是不是应该放在 if 内， snapshot成功之后，再进行状态的更新? 这里相当于先更新了
					if _, err := d.peerStorage.ApplySnapshot(&ready.Snapshot); err == nil {
						d.peerStorage.applyState.TruncatedState.Index = ready.Snapshot.Metadata.Index
						d.peerStorage.applyState.TruncatedState.Term = ready.Snapshot.Metadata.Term
						// snapshot kv 全部更新了，所以必须重新apply
						d.peerStorage.applyState.AppliedIndex = ready.Snapshot.Metadata.Index
						//if len(ready.Entries) == 0 {

						d.peerStorage.raftState.LastIndex = ready.SnapshoRaftStateIndex
						d.peerStorage.raftState.LastTerm = ready.SnapshoRaftStateTerm
						raftbatch := engine_util.WriteBatch{}
						raftbatch.SetMeta(meta.RaftStateKey(d.regionId), d.peerStorage.raftState)
						raftbatch.MustWriteToDB(d.peerStorage.Engines.Raft)

						//
						d.ctx.storeMeta.Lock()
						d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
						d.ctx.storeMeta.regions[d.regionId] = d.Region()
						d.ctx.storeMeta.Unlock()
						//d.RaftGroup.Raft.HandleSnapshotAgain(*ready.Snapshot.Metadata)
					}
				}
			} else {
				_, err := d.peerStorage.SaveReadyState(&ready)
				if err != nil {
					panic(" what happened")
				}

				for _, item := range ready.CommittedEntries {
					applystatewrite = true
					if item.Data != nil {
						if item.EntryType == eraftpb.EntryType_EntryNormal {
							cmdreq := raft_cmdpb.RaftCmdRequest{}
							err := proto.Unmarshal(item.Data, &cmdreq)
							if err != nil {
								panic(" unmarshl error")
							}
							d.commitRaftCmdRequest(item.Index, item.Term, &cmdreq)
						} else if item.EntryType == eraftpb.EntryType_EntryConfChange {
							confchag := eraftpb.ConfChange{}
							err := proto.Unmarshal(item.Data, &confchag)
							if err != nil {
								panic(" unmarshl confchange error")
							}
							d.processConfChange(item.Index, confchag, 0)
						}
					}
				}
				if len(ready.CommittedEntries) > 0 {
					d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
				}
			}

			// write applystate, region state
			regionpeerstate := rspb.PeerState_Normal
			{
				//mylog.Printf(mylog.LevelBaisc, " peer %s after confchange peers %v", d.Tag, d.RaftGroup.Raft.Peers)
				for _, raftpeer := range d.RaftGroup.Raft.Peers {
					if find, _ := d.findpeerinregionpeers(raftpeer); !find {
						var peernew *metapb.Peer
						if peerinfo := d.getPeerFromCache(raftpeer); peerinfo != nil {
							peernew = &metapb.Peer{
								Id:      peerinfo.Id,
								StoreId: peerinfo.StoreId,
							}
						} else if raftpeer == d.peer.PeerId() {
							peernew = d.Meta
						}
						if peernew != nil {
							d.Region().Peers = append(d.Region().Peers, peernew)
						}
					}
				}
				applystatewrite = true
				/// 刚刚重新创建，peers 为空，同时truncated index 和 term 都是0， 而不是5，这个时候如果写下去，然后重启了 这个peer无法重启 region 中没有 storeid 对应的peer
				if len(d.Region().Peers) > 0 || d.peerStorage.applyState.TruncatedState.Index > 0 {
					meta.WriteRegionState(&KvBatch, d.Region(), regionpeerstate)
				}
				//meta.WriteRegionState(&KvBatch, d.Region(), regionpeerstate)
			}

			if applystatewrite == true {
				KvBatch.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			}
			KvBatch.MustWriteToDB(d.peerStorage.Engines.Kv)

			// log print
			if ready.Snapshot.Metadata != nil {
				mylog.Printf(mylog.LevelCompactSnapshot, "%s", d.getraftinfo(true))
			} else {
				mylog.Printf(mylog.LevelAppendEntry, "%s", d.getraftinfo(false))
			}
			if d.peerStorage.raftState.LastIndex < d.peerStorage.applyState.AppliedIndex {
				entry, err := meta.GetRaftEntry(d.peerStorage.Engines.Raft, d.regionId, d.peerStorage.applyState.AppliedIndex)
				log.Errorf("peer %s entry %v err %v   lastinx %d < applyiedinx %d \n", d.Tag, entry, err,
					d.peerStorage.raftState.LastIndex, d.peerStorage.applyState.AppliedIndex)
				panic(" error happend")
			}
		}

		for _, item := range ready.Messages {
			err := d.peer.sendRaftMessage(item, d.ctx.trans)
			if err != nil {
				log.Infof(" peer %s  sendmsg to %d error %v \r\n ", d.Tag, item.To, err)
			} else {
				log.Infof(" peer %s  sendmsg to %d \r\n ", d.Tag, item.To)
			}
		}

		d.RaftGroup.Advance(ready)
	}
	//d.proposals
}

func (d *peerMsgHandler) processConfChange(index uint64, confchag eraftpb.ConfChange, storeid uint64) {
	bfind, _ := d.findpeerinraftpeers(confchag.NodeId)
	reginfind, regioninx := d.findpeerinregionpeers(confchag.NodeId)

	mylog.Printf(mylog.LevelBaisc, "peer %s apply index confchange %d start confchange %v store %d now: regionpeers %v raftpeers %v ",
		d.Tag, index, confchag, storeid, d.Region().Peers, d.RaftGroup.Raft.Peers)
	if confchag.ChangeType == eraftpb.ConfChangeType_AddNode && bfind == false {
		addpeer := &metapb.Peer{
			Id:      confchag.NodeId,
			StoreId: storeid,
		}
		if reginfind == false {
			d.Region().Peers = append(d.Region().Peers, addpeer)
		}
		d.Region().RegionEpoch.ConfVer += 1
		d.insertPeerCache(addpeer)
	} else if confchag.ChangeType == eraftpb.ConfChangeType_RemoveNode && bfind == true {
		if reginfind {
			d.Region().Peers = append(d.Region().Peers[:regioninx], d.Region().Peers[regioninx+1:]...)
		}
		d.Region().RegionEpoch.ConfVer += 1
		d.removePeerCache(confchag.NodeId)
		if d.PeerId() == confchag.NodeId {
			mylog.Printf(mylog.LevelBaisc, "peer %s will destroyPeer killmyself ", d.Tag)
			d.ctx.router.send(d.regionId, message.Msg{
				RegionID: d.regionId,
				Type:     message.MsgTypePeerKillMyself,
			})
		}
	} else {
		mylog.Printf(mylog.LevelBaisc, "peer %s confchange discard", d.Tag)
		return
	}
	d.RaftGroup.ApplyConfChange(confchag)
	mylog.Printf(mylog.LevelBaisc, "peer %s after confchange region raftpeers %v  ppers %v ", d.Tag, d.RaftGroup.Raft.Peers, d.Region().Peers)
}

func (d *peerMsgHandler) splitfindnewpeers(splitreq *raft_cmdpb.SplitRequest) []*metapb.Peer {
	if len(d.Region().Peers) != len(splitreq.NewPeerIds) {
		panic(fmt.Sprintf("peer %s need split can not find %d  %v newpeerid %v ",
			d.Tag, d.Region().Id, d.Region().Peers, splitreq.NewPeerIds))
	}

	stores := []uint64{}
	for _, item := range d.Region().Peers {
		stores = append(stores, item.StoreId)
	}
	sortkeys.Uint64s(stores)
	var peeers []*metapb.Peer
	for inx, storeid := range stores {
		peeers = append(peeers, &metapb.Peer{
			Id:      splitreq.NewPeerIds[inx],
			StoreId: storeid,
		})
	}
	return peeers
}
func (d *peerMsgHandler) processSplit(index uint64, term uint64, splitreq *raft_cmdpb.SplitRequest) error {
	/*
		2. Split 的时候，原 region 与新 region 的 version 均等于原 region 的 version + 新 region 个数。
		3. Merge 的时候，两个 region 的 version 均等于这两个 region 的 version 最大值 + 1。
		2 和 3 两个规则可以推出一个有趣的结论：如果两个 Region 拥有的范围有重叠，只需比较两者的 version 即可确认两者之间的历史先后顺序，version 大的意味着更新，不存在相等的情况。
		证明比较简单，由于范围只在 Split 和 Merge 的时候才会改变，而每一次的 Split 和 Merge 都会更新影响到的范围里 Region 的 version，并且更新到比原范围中的 version 更大，
		对于一段范围来说，不管它属于哪个 Region，它所在 Region 的 version 一定是严格单调递增的。PD 使用了这个规则去判断范围重叠的不同 Region 的新旧。*/

	//engine_util.ExceedEndKey(nil, nil)
	//util.ErrRegionNotFound
	mylog.Printf(mylog.LevelCompactSnapshot, "peer %s apply index split %d  oldregion %d newregion %d splitkeys %s newpeersid %v  [%s-%s) [%s-%s)",
		d.Tag, index, d.regionId, splitreq.NewRegionId, splitreq.GetSplitKey(), splitreq.NewPeerIds,
		string(d.Region().StartKey), string(splitreq.SplitKey), string(splitreq.SplitKey), string(d.Region().EndKey))

	originalendkey := util.SafeCopy(d.Region().EndKey)

	d.Region().EndKey = util.SafeCopy(splitreq.SplitKey)
	d.Region().RegionEpoch.Version += 1
	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	d.ctx.storeMeta.Unlock()

	if true {
		d.ctx.storeMeta.Lock()
		if _, ok := d.ctx.storeMeta.regions[splitreq.NewRegionId]; ok {
			d.ctx.storeMeta.Unlock()
			return nil
		}
		newregion := new(metapb.Region)
		newregion.Id = splitreq.NewRegionId
		newregion.StartKey = util.SafeCopy(splitreq.SplitKey)
		newregion.EndKey = originalendkey
		newregion.RegionEpoch = &metapb.RegionEpoch{
			ConfVer: d.Region().RegionEpoch.ConfVer,
			Version: d.Region().RegionEpoch.Version,
		}
		newregion.Peers = d.splitfindnewpeers(splitreq)
		newpeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newregion)
		if err != nil {
			panic(fmt.Sprintf("create peer error %v", err))
		}
		d.ctx.router.register(newpeer)
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newpeer.Region()})
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		d.ctx.storeMeta.regions[newpeer.regionId] = newpeer.Region()
		d.ctx.storeMeta.Unlock()

		//d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		//newpeer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		if d.IsLeader() {
			newpeer.MaybeCampaign(d.IsLeader())
		}
		_ = d.ctx.router.send(newpeer.regionId, message.Msg{RegionID: newpeer.regionId, Type: message.MsgTypeStart})
	}

	return nil
}

func (d *peerMsgHandler) commitRaftCmdRequest(index uint64, term uint64, req *raft_cmdpb.RaftCmdRequest) {
	cmdresp := raft_cmdpb.RaftCmdResponse{}
	//startkey, _ := d.Region().StartKey, d.Region().EndKey
	var errret error
	/*if( req.Header == nil || req.Header.RegionEpoch == nil){
		panic(" stupid ")
	}*/
	if req.Header != nil && req.Header.RegionEpoch != nil && util.IsEpochStale(req.Header.RegionEpoch, d.Region().GetRegionEpoch()) {
		errret = &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("epoch is stale "),
			Regions: []*metapb.Region{d.Region()},
		}
		mylog.Printf(mylog.LevelAppendEntry, "%s apply index %d term %d epoch not match %d-%d , %d-%d ",
			d.Tag, index, term, req.Header.RegionEpoch.Version, req.Header.RegionEpoch.ConfVer,
			d.Region().RegionEpoch.Version, d.Region().RegionEpoch.ConfVer)

	} else {
		for _, item := range req.Requests {
			resp := raft_cmdpb.Response{CmdType: item.CmdType}
			switch item.CmdType {
			case raft_cmdpb.CmdType_Get:
				{
					if errret = util.CheckKeyInRegion(item.Get.Key, d.Region()); errret != nil {
						break
					}

					resp.Get = &raft_cmdpb.GetResponse{}
					val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, item.Get.Cf, item.Get.Key)
					mylog.Printf(mylog.LevelAppendEntry, "peer %s apply index get %d key %v  %v  return val %v err %v ",
						d.Tag, index, string(item.Get.Cf), string(item.Get.Key), string(val), err)
					if err == nil {
						resp.Get.Value = val
					} else {
						errret = err
					}
					break
				}
			case raft_cmdpb.CmdType_Put:
				{
					if errret = util.CheckKeyInRegion(item.Put.Key, d.Region()); errret != nil {
						break
					}

					err := engine_util.PutCF(d.peerStorage.Engines.Kv, item.Put.Cf, item.Put.Key, item.Put.Value)
					mylog.Printf(mylog.LevelAppendEntry, "peer %s apply index put %d cf%s key %v   val %v err %v ",
						d.Tag, index, string(item.Put.Cf), string(item.Put.Key), string(item.Put.Value), err)
					break
				}
			case raft_cmdpb.CmdType_Delete:
				{
					if errret = util.CheckKeyInRegion(item.Delete.Key, d.Region()); errret != nil {
						break
					}
					err := engine_util.DeleteCF(d.peerStorage.Engines.Kv, item.Delete.Cf, item.Delete.Key)
					mylog.Printf(mylog.LevelAppendEntry, "peer %s apply index delete %d key %v %v  verr %v ", d.Tag, index, string(item.Delete.Cf), string(item.Delete.Key), err)
					break
				}
			case raft_cmdpb.CmdType_Snap:
				{
					resp.Snap = &raft_cmdpb.SnapResponse{
						Region: &metapb.Region{
							Id:       d.regionId,
							StartKey: d.Region().StartKey,
							EndKey:   d.Region().EndKey,
							RegionEpoch: &metapb.RegionEpoch{
								ConfVer: d.Region().RegionEpoch.ConfVer,
								Version: d.Region().RegionEpoch.Version,
							},
						},
					}
					mylog.Printf(mylog.LevelAppendEntry, "peer %s apply index snap %d region %d start %v end %v", d.Tag, index,
						resp.Snap.Region.Id, string(resp.Snap.Region.StartKey), string(resp.Snap.Region.EndKey))
					break
				}
			default:
				{
					panic(" invalid method")
				}
			}
			cmdresp.Responses = append(cmdresp.Responses, &resp)
		}
		if req.AdminRequest != nil {
			cmdresp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType: req.AdminRequest.CmdType,
			}
			switch req.AdminRequest.CmdType {
			case raft_cmdpb.AdminCmdType_CompactLog:
				{
					mylog.Printf(mylog.LevelAppendEntry, "peer %s apply index compactlog %d from index %d term %d", d.Tag, index,
						req.AdminRequest.CompactLog.CompactIndex, req.AdminRequest.CompactLog.CompactTerm)

					cmdresp.AdminResponse.CompactLog = &raft_cmdpb.CompactLogResponse{}
					err, compactinx, compactterm := d.RaftGroup.Raft.RaftLog.MaybeCompact(req.AdminRequest.CompactLog.CompactIndex, req.AdminRequest.CompactLog.CompactTerm)
					if err != nil {
						errret = err
						mylog.Printf(mylog.LevelCompactSnapshot, "peer %s apply compact to index %d term %d sourceinx term %d %d err %v ", d.Tag, compactinx, compactterm,
							req.AdminRequest.CompactLog.CompactIndex, req.AdminRequest.CompactLog.CompactTerm, err)
					} else {
						d.peerStorage.applyState.TruncatedState.Index = compactinx
						d.peerStorage.applyState.TruncatedState.Term = compactterm

						batch := engine_util.WriteBatch{}
						batch.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
						batch.MustWriteToDB(d.peerStorage.Engines.Kv)

						d.ScheduleCompactLog(compactinx)
						mylog.Printf(mylog.LevelCompactSnapshot, "peer %s apply compact to index %d term %d sourceinx term %d %d", d.Tag, compactinx, compactterm,
							req.AdminRequest.CompactLog.CompactIndex, req.AdminRequest.CompactLog.CompactTerm)
					}
					break
				}
			case raft_cmdpb.AdminCmdType_ChangePeer:
				{
					confchange := eraftpb.ConfChange{
						NodeId:     req.AdminRequest.ChangePeer.Peer.Id,
						ChangeType: req.AdminRequest.ChangePeer.ChangeType,
					}
					d.processConfChange(index, confchange, req.AdminRequest.ChangePeer.Peer.StoreId)
				}
			case raft_cmdpb.AdminCmdType_Split:
				{
					errret = d.processSplit(index, term, req.AdminRequest.Split)
					if errret == nil {
						if newregion, has := d.ctx.storeMeta.regions[req.AdminRequest.Split.NewRegionId]; has {
							cmdresp.AdminResponse.Split = &raft_cmdpb.SplitResponse{
								Regions: []*metapb.Region{newregion},
							}
						} else if d.IsLeader() {
							panic(" leader after split no new region")
						}
					}
				}
			default:
				{
					panic(" panic not implented")
				}

			}
		}
	}
	ensureRespHeader(&cmdresp)
	if errret != nil {
		BindRespError(&cmdresp, errret)
	}
	cmdresp.Header.CurrentTerm = d.RaftGroup.Raft.Term

	for i := 0; i < len(d.proposals); { //, posal := range d.proposals {
		posal := d.proposals[i]
		if posal.index == index {
			if posal.cb != nil {
				if posal.term == term {
					posal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
					posal.cb.Done(&cmdresp)
				} else {
					posal.cb.Done(ErrRespStaleCommand(posal.term))
				}
			}
			// delete the index
			d.proposals = append(d.proposals[:i], d.proposals[i+1:]...)

			//break
		} else {
			i++
		}
	}
	for _, posal2 := range d.proposals {
		if posal2.index == index {
			panic(" two same index ????")
		}
	}

	if req.AdminRequest != nil {

	}
}
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			mylog.Basic("%s handle raft message error %v from %v type %v ", d.Tag, err, raftMsg.FromPeer.Id, raftMsg.Message.MsgType)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()

	case message.MsgTypePeerKillMyself:
		if bfind, _ := d.findpeerinraftpeers(d.PeerId()); bfind == false && d.MaybeDestroy() {
			d.destroyPeer()
		}
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		{
			// 处理pending votes消息， peer创建完成之后，就开始处理pending的消息
			d.startTicker()
			d.ctx.storeMeta.Lock()
			for inx := 0; inx < len(d.ctx.storeMeta.pendingVotes); {
				if util.PeerEqual(d.ctx.storeMeta.pendingVotes[inx].ToPeer, d.Meta) {
					raftMsg := d.ctx.storeMeta.pendingVotes[inx]
					mylog.Basic("peer %s process pendingvotes from %s ", d.Tag, raftMsg.FromPeer)
					if err := d.onRaftMsg(raftMsg); err != nil {
						mylog.Basic("%s handle raft message error %v from %v type %v ", d.Tag, err, raftMsg.FromPeer.Id, raftMsg.Message.MsgType)
					}
					d.ctx.storeMeta.pendingVotes = append(d.ctx.storeMeta.pendingVotes[:inx], d.ctx.storeMeta.pendingVotes[inx+1:]...)
				} else {
					inx++
				}
			}
			d.ctx.storeMeta.Unlock()
		}
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	raftcmdrequest, err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		}
		cb.Done(resp)
	} else {
		inx1, _ := d.RaftGroup.LastIndexAndTerm()
		err = d.RaftGroup.Propose(raftcmdrequest)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		inx, term := d.RaftGroup.LastIndexAndTerm()

		d.RaftGroup.Raft.Printraftcmdrequest(inx, msg, false)
		if err == nil && inx != inx1+1 {
			panic(fmt.Sprintf(" error happend %d %d  msg %v ", inx1, inx, msg))
		}

		d.proposals = append(d.proposals, &proposal{
			index: inx,
			term:  term,
			cb:    cb,
		})
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		mylog.Printf(mylog.LevelBaisc, "peer %s get tombstone maybe destroy myepoch %v  from epoch %v, mypeer %v  frompeer %v",
			d.Tag, d.Region().RegionEpoch, msg.RegionEpoch, d.Meta, msg.FromPeer)
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			mylog.Basic("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy ", d.Tag)
	mylog.Printf(mylog.LevelBaisc, "%s starts destroy region : %s", d.Tag, d.RegionStr())
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func (d *peerMsgHandler) RegionStr() string {
	return fmt.Sprintf(" id : %d, epoch %d-%d keys[%s-%s) peers %v", d.Region().Id, d.Region().RegionEpoch.Version, d.Region().RegionEpoch.ConfVer,
		mylog.GetString(d.Region().StartKey), mylog.GetString(d.Region().EndKey), d.Region().Peers)
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
