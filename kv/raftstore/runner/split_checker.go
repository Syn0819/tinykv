package runner

import (
	"encoding/hex"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type SplitCheckTask struct {
	Region *metapb.Region
}

type splitCheckHandler struct {
	engine  *badger.DB
	router  message.RaftRouter
	checker *sizeSplitChecker
}

func NewSplitCheckHandler(engine *badger.DB, router message.RaftRouter, conf *config.Config) *splitCheckHandler {
	runner := &splitCheckHandler{
		engine:  engine,
		router:  router,
		checker: newSizeSplitChecker(conf.RegionMaxSize, conf.RegionSplitSize),
	}
	return runner
}

/// run checks a region with split checkers to produce split keys and generates split admin command.
func (r *splitCheckHandler) Handle(t worker.Task) {
	log.Infof("Handle, start splitCheckHandler")
	spCheckTask, ok := t.(*SplitCheckTask) // 空接口获取值
	if !ok {
		log.Errorf("unsupported worker.Task: %+v", t)
		return
	}
	region := spCheckTask.Region
	regionId := region.Id
	log.Infof("executing split check worker.Task: [regionId: %d, startKey: %s, endKey: %s]", regionId,
		hex.EncodeToString(region.StartKey), hex.EncodeToString(region.EndKey))
	key := r.splitCheck(regionId, region.StartKey, region.EndKey)
	if key != nil {
		// 检查可split
		_, userKey, err := codec.DecodeBytes(key)
		if err == nil {
			// It's not a raw key.
			// To make sure the keys of same user key locate in one Region, decode and then encode to truncate the timestamp
			key = codec.EncodeBytes(userKey)
		}
		msg := message.Msg{
			Type:     message.MsgTypeSplitRegion,
			RegionID: regionId,
			Data: &message.MsgSplitRegion{
				RegionEpoch: region.GetRegionEpoch(),
				SplitKey:    key,
			},
		}
		err = r.router.Send(regionId, msg)
		if err != nil {
			log.Warnf("failed to send check result: [regionId: %d, err: %v]", regionId, err)
		}
	} else {
		log.Infof("no need to send, split key not found: [regionId: %v]", regionId)
	}
}

/// SplitCheck gets the split keys by scanning the range.
// 获取split key，方式是遍历key范围
func (r *splitCheckHandler) splitCheck(regionID uint64, startKey, endKey []byte) []byte {
	txn := r.engine.NewTransaction(false)
	defer txn.Discard()

	r.checker.reset()
	it := engine_util.NewCFIterator(engine_util.CfDefault, txn)
	defer it.Close()
	// 检查
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		// 有两种情况，可以产生split key
		// 1. 存储的key大小超过当前region的endkey
		// 2. 存储的k+v长度超过阈值
		log.Infof("splitCheck, status 1, key: %v, endKey: %v", key, endKey)
		if engine_util.ExceedEndKey(key, endKey) {
			// update region size
			r.router.Send(regionID, message.Msg{
				Type: message.MsgTypeRegionApproximateSize,
				Data: r.checker.currentSize,
			})
			break
		}
		// 给未split检查
		if r.checker.onKv(key, item) {
			break
		}
	}
	return r.checker.getSplitKey()
}

type sizeSplitChecker struct {
	// maxSize 和 splitSize是设定的参数
	maxSize   uint64
	splitSize uint64

	currentSize uint64
	splitKey    []byte
}

func newSizeSplitChecker(maxSize, splitSize uint64) *sizeSplitChecker {
	return &sizeSplitChecker{
		maxSize:   maxSize,
		splitSize: splitSize,
	}
}

func (checker *sizeSplitChecker) reset() {
	checker.currentSize = 0
	checker.splitKey = nil
}

// 检查key+value的长度
// 如果当前region内k+v长度大于splitSize，那么需要记录下当前key，以作为需要split的endkey
func (checker *sizeSplitChecker) onKv(key []byte, item engine_util.DBItem) bool {
	valueSize := uint64(item.ValueSize())
	size := uint64(len(key)) + valueSize
	checker.currentSize += size
	log.Infof("onKv, status 2, currentSize: %v, splitSize: %v, maxSize: %v",
		checker.currentSize, checker.splitSize, checker.maxSize)
	log.Infof("onKv, splitKey: %v", checker.splitKey)
	if checker.currentSize > checker.splitSize && checker.splitKey == nil {
		checker.splitKey = util.SafeCopy(key)
	}
	return checker.currentSize > checker.maxSize
}

// 这里主要是check一下目前region key的大小需要超过设定阈值
func (checker *sizeSplitChecker) getSplitKey() []byte {
	// Make sure not to split when less than maxSize for last part
	if checker.currentSize < checker.maxSize {
		checker.splitKey = nil
	}
	log.Infof("getSplitKey, splitKey: %v", checker.splitKey)
	return checker.splitKey
}
