package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

// get request key from normal request
func (d *peerMsgHandler) getRequestKey(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	case raft_cmdpb.CmdType_Snap:
	}
	var Reqkey []byte
	return Reqkey
}

func (d *peerMsgHandler) handleAdminRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) {
	req := msg.AdminRequest
	log.Infof("handleAdminRequest, req.CmdType: %v", req.CmdType)
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		// 处理逻辑：
		// 1. 检查snap触发的CompactIndex是否比之前截断的log index大，不然没意义
		campactLog := req.GetCompactLog()
		applyState := d.peerStorage.applyState
		if campactLog.CompactIndex >= applyState.TruncatedState.Index {
			applyState.TruncatedState.Index = campactLog.CompactIndex
			applyState.TruncatedState.Term = campactLog.CompactTerm

			log.Infof("handleAdminRequest, applyState.TruncatedState.Index:%v, applyState.TruncatedState.Term:%v",
				applyState.TruncatedState.Index, applyState.TruncatedState.Term)

			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
			d.ScheduleCompactLog(applyState.TruncatedState.Index)
		}
	case raft_cmdpb.AdminCmdType_ChangePeer:
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
	case raft_cmdpb.AdminCmdType_Split:
		/* 处理逻辑: split需要将region分裂成两个，一个继承原来的，一个新创建
		** 1. 读取被分离的region信息
		** 2. 读取分裂的region信息
		** 3. 从被分离的region中删除分裂region的信息
		** 4. 新建分裂的region信息
		** 5. region信息写入引擎
		 */
		region := d.Region()
		err := util.CheckRegionEpoch(msg, region, true)
		if errEpochNotMatch, ok := err.(*util.ErrRegionNotFound); ok {
			if len(d.proposals) > 0 {
				p := d.proposals[0]
				if p.index == entry.Index {
					if p.term == entry.Term {
						// find entry
						p.cb.Done(ErrResp(errEpochNotMatch))
						return
					}
					NotifyStaleReq(entry.Term, p.cb)
				}
			}
		}

		split := req.GetSplit()
		err = util.CheckKeyInRegion(split.SplitKey, region)
		if err != nil {
			if len(d.proposals) > 0 {
				p := d.proposals[0]
				if p.index == entry.Index {
					if p.term == entry.Term {
						// find entry
						p.cb.Done(ErrResp(err))
						return
					}
					NotifyStaleReq(entry.Term, p.cb)
				}
			}
		}
		// 修改meta信息
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regionRanges.Delete(&regionItem{region: region})
		region.RegionEpoch.Version++

		newPeers := make([]*metapb.Peer, 0)
		for i, peer := range region.Peers {
			newPeers = append(newPeers, &metapb.Peer{Id: split.NewPeerIds[i], StoreId: peer.StoreId})
		}
		newRegion := &metapb.Region{
			Id:       split.NewRegionId,
			StartKey: split.SplitKey,
			EndKey:   region.EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: newPeers,
		}
		storeMeta.regions[split.NewRegionId] = newRegion

		region.EndKey = split.SplitKey

		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		storeMeta.Unlock()

		// 持久化新 旧region信息
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)

		// 真正的去创建新peer并且注册进入router
		peer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.schedulerTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		d.ctx.router.register(peer)
		d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
		if len(d.proposals) > 0 {
			p := d.proposals[0]
			if p.index == entry.Index {
				if p.term == entry.Term {
					// find entry
					p.cb.Done(&raft_cmdpb.RaftCmdResponse{
						Header: &raft_cmdpb.RaftResponseHeader{},
						AdminResponse: &raft_cmdpb.AdminResponse{
							CmdType: raft_cmdpb.AdminCmdType_Split,
							Split: &raft_cmdpb.SplitResponse{
								Regions: []*metapb.Region{region, newRegion},
							},
						},
					})
					return
				}
				NotifyStaleReq(entry.Term, p.cb)
			}
		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
	}

}

//
func (d *peerMsgHandler) handleRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.Requests[0]
	key := d.getRequestKey(req)
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			if len(d.proposals) > 0 {
				p := d.proposals[0]
				if p.index == entry.Index {
					if p.term == entry.Term {
						p.cb.Done(ErrResp(err))
					} else {
						NotifyStaleReq(entry.Term, p.cb)
					}
				}
			}
		}
	}
	// real handle request into kv

	// 读操作不需要前后缀
	switch req.CmdType {
	case raft_cmdpb.CmdType_Delete:
		kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
	case raft_cmdpb.CmdType_Put:
		kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	case raft_cmdpb.CmdType_Get:
	case raft_cmdpb.CmdType_Snap:
	}

	if len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index == entry.Index {
			if p.term == entry.Term {
				resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
				switch req.CmdType {
				case raft_cmdpb.CmdType_Get:
					d.peerStorage.applyState.AppliedIndex = entry.Index
					kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
					kvWB.WriteToDB(d.peerStorage.Engines.Kv)
					value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
					if err != nil {
						value = nil
					}
					resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: value}}}
					kvWB = new(engine_util.WriteBatch)
				case raft_cmdpb.CmdType_Put:
					resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}}}
				case raft_cmdpb.CmdType_Delete:
					resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}}}
				case raft_cmdpb.CmdType_Snap:
					if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
						p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
						return kvWB
					}
					d.peerStorage.applyState.AppliedIndex = entry.Index
					kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
					kvWB.WriteToDB(d.peerStorage.Engines.Kv)
					resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
					p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
					kvWB = new(engine_util.WriteBatch)
				}
				p.cb.Done(resp)
			}
		} else {
			NotifyStaleReq(entry.Term, p.cb)
		}
		d.proposals = d.proposals[1:]
	}
	return kvWB
}

func (d *peerMsgHandler) searchAPeerInRegion(region *metapb.Region, id uint64) int {
	for i, peer := range region.Peers {
		if peer.Id == id {
			return i
		}
	}
	return len(region.Peers)
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) handleConfChange(entry *eraftpb.Entry, cc *eraftpb.ConfChange, kvWB *engine_util.WriteBatch) {
	// 具体处理逻辑
	// 修改 RegionLocalState 中的状态，包括 RegionEpoch，peers
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	region := d.Region()
	err = util.CheckRegionEpoch(msg, region, true)
	if errEpochNotMatch, ok := err.(*util.ErrEpochNotMatch); ok {
		//
		if len(d.proposals) > 0 {
			p := d.proposals[0]
			if p.index == entry.Index {
				if p.term == entry.Term {
					// 日志check, 确认该日志不在region内，回复错误
					p.cb.Done(ErrResp(errEpochNotMatch))
				} else {
					NotifyStaleReq(entry.Term, p.cb)
				}
			}
			d.proposals = d.proposals[1:]
		}

	}

	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		// 增加节点，看下region内是否存在该节点
		index := d.searchAPeerInRegion(region, cc.NodeId)
		if index == len(region.Peers) {
			// 添加进peer
			peer := msg.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers, peer)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)

			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.insertPeerCache(peer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		// 删除节点
		// 若要删除的是本节点，则调用destroyPeer
		if cc.NodeId == d.Meta.Id {
			d.destroyPeer()
			return
		}

		index := d.searchAPeerInRegion(region, cc.NodeId)
		if index < len(region.Peers) {
			region.Peers = append(region.Peers[:index], region.Peers[index+1:]...)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)

			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.removePeerCache(cc.NodeId)
		}
	}

	// raft模块实际处理conf change
	d.RaftGroup.ApplyConfChange(*cc)

	d.notifyHeartbeatScheduler(region, d.peer)
	// 回响应
	if len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index == entry.Index {
			if p.term == entry.Term {
				p.cb.Done(&raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					AdminResponse: &raft_cmdpb.AdminResponse{
						CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
						ChangePeer: &raft_cmdpb.ChangePeerResponse{},
					},
				})

			} else {
				NotifyStaleReq(entry.Term, p.cb)
			}
		}
		d.proposals = d.proposals[1:]
	}
}

func (d *peerMsgHandler) applyEntries(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	// 处理confchange，即节点add或remove
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := &eraftpb.ConfChange{}
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		d.handleConfChange(entry, cc, kvWB)
		return kvWB
	}
	// unmarshal
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(entry.Data)
	if err != nil {
		panic(err)
	}
	// real handle request in db
	if len(msg.Requests) > 0 {
		return d.handleRequest(entry, msg, kvWB)
	}

	if msg.AdminRequest != nil {
		d.handleAdminRequest(entry, msg, kvWB)
		return kvWB
	}

	return kvWB
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	// persisting log entries, applying committed entries
	// sending raft messages to other peers though network
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()

		result, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			panic(err)
		}
		if result != nil {
			if !reflect.DeepEqual(result.PrevRegion, result.Region) {
				d.peerStorage.SetRegion(result.Region)
				storeMeta := d.ctx.storeMeta
				storeMeta.Lock()
				storeMeta.regions[result.Region.Id] = result.Region
				storeMeta.regionRanges.Delete(&regionItem{region: result.PrevRegion})
				storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
				storeMeta.Unlock()
			}
		}

		d.Send(d.ctx.trans, ready.Messages)
		if len(ready.CommittedEntries) > 0 {
			oldProposals := d.proposals
			// apply entries
			kvWB := new(engine_util.WriteBatch)
			for _, entry := range ready.CommittedEntries {
				// unmarshal
				// 对于每个raft模块已提交的日志，需要写入badger
				kvWB = d.applyEntries(&entry, kvWB)
				if d.stopped {
					return
				}
			}
			// finish apply, update state
			d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)

			if len(d.proposals) < len(oldProposals) {
				proposals := make([]*proposal, len(d.proposals))
				copy(proposals, d.proposals)
				d.proposals = proposals
			}
		}
		d.RaftGroup.Advance(ready)
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	log.Infof("HandleMsg, message type: %v", msg.Type)
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
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
		d.startTicker()
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

// handle admin request
func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	log.Infof("proposeAdminRequest, req.CmdType:%v", req.CmdType)
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		//
		context, err := msg.Marshal()
		if err != nil {
			panic(err)
		}

		cc := eraftpb.ConfChange{
			ChangeType: req.ChangePeer.ChangeType,
			NodeId:     req.ChangePeer.Peer.Id,
			Context:    context,
		}

		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)

		d.RaftGroup.ProposeConfChange(cc)
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// 这个leader转移的操作，虽然属于raft command的一种
		// 但是实际就做了一个转移动作，不需要将日志复制给peers
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)

		transferLeaderRequest := raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		}

		cb.Done(&transferLeaderRequest)
	case raft_cmdpb.AdminCmdType_Split:
		err := util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}

		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}

		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)

		d.RaftGroup.Propose(data)
	}
}

// handle normal request
func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// one proposal per round
	var key []byte
	switch msg.Requests[0].CmdType {
	case raft_cmdpb.CmdType_Delete:
		key = msg.Requests[0].Delete.Key
	case raft_cmdpb.CmdType_Get:
		key = msg.Requests[0].Get.Key
	case raft_cmdpb.CmdType_Put:
		key = msg.Requests[0].Put.Key
	}

	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}

	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}

	// 每一次propose对应一个唯一的[index term]二元组
	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})
	d.RaftGroup.Propose(data)
}

// 处理raft command
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.Requests != nil {
		d.proposeRequest(msg, cb)
	} else {
		d.proposeAdminRequest(msg, cb)
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
	log.Infof("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
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

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
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
// 检查msg是否包含快照，或其快照是否与其他快照和regions冲突
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
	// 检查快照接收方是否在该region内
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
			log.Infof("%s stale delegate detected, skip", d.Tag)
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
	log.Infof("%s starts destroy", d.Tag)
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
	log.Infof("destroyPeer, isInitialized: %d, region: %d", isInitialized, d.Region())
	log.Infof("destroyPeer, regionRanges size: %d", meta.regionRanges.Len())
	meta.regionRanges.Ascend(func(i btree.Item) bool {
		log.Infof("destroyPeer, search regionRanges, regionID: %d, startKey: %d, endKey: %d", i.(*regionItem).region.Id, i.(*regionItem).region.StartKey, i.(*regionItem).region.EndKey)
		return true
	})
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
	log.Infof("onRaftGCLogTick, appliedIdx:%v, firstIdx:%v, RaftLogGcCountLimit:%v",
		appliedIdx, firstIdx, d.ctx.cfg.RaftLogGcCountLimit)
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
	log.Infof("onRaftGCLogTick, Create a compact log request and notify directly")
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	// 为了避免频繁扫描，只在上一个扫描完成后进行下一次扫描
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	// 只有leader节点能进行split扫描
	if !d.IsLeader() {
		return
	}
	//
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	// 发送任务至split_checker.go
	log.Infof("onSplitRegionCheckTick, SplitCheckTask send...")
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	log.Infof("onPrepareSplitRegion")
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
