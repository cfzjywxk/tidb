// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"
	"github.com/ngaut/log"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/zap"
)

const amendableType = (1 << model.ActionAddColumn) | (1 << model.ActionDropColumn) | (memBufAmendType)
const memBufAmendType = uint64(1 << model.ActionAddIndex)

// Amend operation types
const (
	AmendNone int = iota

	// For add index
	AmendNeedAddDelete
	AmendNeedAddDeleteAndInsert
	AmendNeedAddInsert

	// For drop index
	AmendNeedRemoveInsert
	AmendNeedRemoveInsertAndDelete
	AmendNeedRemoveDelete
)

var constOpAddIndex = map[model.SchemaState]map[model.SchemaState]int{
	model.StateNone: {
		model.StateDeleteOnly:          AmendNeedAddDelete,
		model.StateWriteOnly:           AmendNeedAddDeleteAndInsert,
		model.StateWriteReorganization: AmendNeedAddDeleteAndInsert,
		model.StatePublic:              AmendNeedAddDeleteAndInsert,
	},
	model.StateDeleteOnly: {
		model.StateWriteOnly:           AmendNeedAddInsert,
		model.StateWriteReorganization: AmendNeedAddInsert,
		model.StatePublic:              AmendNeedAddInsert,
	},
	model.StateWriteOnly: {
		model.StateWriteReorganization: AmendNone,
		model.StatePublic:              AmendNone,
	},
	model.StateWriteReorganization: {
		model.StatePublic: AmendNone,
	},
}

var constOpDropIndex = map[model.SchemaState]map[model.SchemaState]int{
	model.StatePublic: {
		model.StateWriteOnly:            AmendNone,
		model.StateDeleteOnly:           AmendNeedRemoveInsert,
		model.StateDeleteReorganization: AmendNeedRemoveInsert,
		model.StateNone:                 AmendNeedRemoveInsertAndDelete,
	},
	model.StateWriteOnly: {
		model.StateDeleteOnly:           AmendNeedRemoveInsert,
		model.StateDeleteReorganization: AmendNeedRemoveInsert,
		model.StateNone:                 AmendNeedRemoveInsertAndDelete,
	},
	model.StateDeleteOnly: {
		model.StateWriteReorganization: AmendNone,
		model.StateNone:                AmendNeedRemoveDelete,
	},
	model.StateDeleteReorganization: {
		model.StateNone: AmendNeedRemoveDelete,
	},
}

// amendCollector collects all amend operations, row decoders and memory chunks for each table needs amend
type amendCollector struct {
	tblAmendOpMap map[int64][]amendOp
	tblDecoder    map[int64]*rowcodec.ChunkDecoder
	tblChk        map[int64]*chunk.Chunk
}

func newAmendCollector() *amendCollector {
	res := &amendCollector{
		tblAmendOpMap: make(map[int64][]amendOp),
		tblDecoder:    make(map[int64]*rowcodec.ChunkDecoder),
		tblChk:        make(map[int64]*chunk.Chunk),
	}
	return res
}

func (a *amendCollector) collectIndexAmendOps(ctx context.Context, dbID, phyTblID int64,
	tblAtStart, tblAtCommit *model.TableInfo) ([]amendOp, error) {
	res := make([]amendOp, 0, 4)
	op := amendOperationAddIndex{}
	op.dbID = dbID
	op.phyTblID = phyTblID
	op.tblInfoAtStart = tblAtStart
	op.tblInfoAtCommit = tblAtCommit
	op.decoder = a.tblDecoder[phyTblID]
	op.chk = a.tblChk[phyTblID]
	op.processedNewIndexKeys = make(map[string]interface{})
	// Check index having state change, collect index column info
	for _, idxInfoAtCommit := range tblAtCommit.Indices {
		op.idxID = idxInfoAtCommit.ID
		var idxInfoAtStart *model.IndexInfo
		for _, oldIndexInfo := range tblAtStart.Indices {
			if oldIndexInfo.ID == idxInfoAtCommit.ID {
				idxInfoAtStart = oldIndexInfo
				break
			}
		}
		// Try to find index state change
		if idxInfoAtStart == nil {
			op.indexInfoAtStart = nil
			op.AmendOpType = constOpAddIndex[model.StateNone][idxInfoAtCommit.State]
		} else if idxInfoAtCommit.State > idxInfoAtStart.State {
			op.indexInfoAtStart = idxInfoAtStart
			op.AmendOpType = constOpAddIndex[idxInfoAtStart.State][idxInfoAtCommit.State]
		}
		op.indexInfoAtCommit = idxInfoAtCommit
		if op.AmendOpType != AmendNone {
			// TODO now index column MUST be found in old table columns
			for _, idxCol := range idxInfoAtCommit.Columns {
				var oldColInfo *model.ColumnInfo
				for _, colInfo := range tblAtStart.Columns {
					if tblAtCommit.Columns[idxCol.Offset].ID == colInfo.ID {
						op.relatedOldIdxCols = append(op.relatedOldIdxCols, colInfo)
						oldColInfo = colInfo
						break
					}
				}
				if oldColInfo == nil {
					return nil, table.ErrUnsupportedOp
				}
			}
			res = append(res, &op)
		}
	}
	return res, nil
}

// collectTblAmendOps collects amend operations for each table using the schema diff between startTS and commitTS
func (a *amendCollector) collectTblAmendOps(ctx context.Context, sctx sessionctx.Context, dbID, phyTblID int64,
	tblInfoAtStart, tblInfoAtCommit *model.TableInfo) error {
	if _, ok := a.tblAmendOpMap[phyTblID]; !ok {
		a.tblAmendOpMap[phyTblID] = make([]amendOp, 0, 4)
		a.tblDecoder[phyTblID] = newRowChkDecoder(sctx, tblInfoAtStart)
		fieldTypes := make([]*types.FieldType, 0, len(tblInfoAtStart.Columns))
		for _, col := range tblInfoAtStart.Columns {
			fieldTypes = append(fieldTypes, &col.FieldType)
		}
		a.tblChk[phyTblID] = chunk.NewChunkWithCapacity(fieldTypes, 4)
	}
	// TODO currently only add index is considered
	ops, err := a.collectIndexAmendOps(ctx, dbID, phyTblID, tblInfoAtStart, tblInfoAtCommit)
	if err != nil {
		return errors.Trace(err)
	}
	a.tblAmendOpMap[phyTblID] = append(a.tblAmendOpMap[phyTblID], ops...)
	return nil
}

func isDeleteOp(keyOp pb.Op) bool {
	return keyOp == pb.Op_Del
}

func isInsertOp(keyOp pb.Op) bool {
	return keyOp == pb.Op_Put || keyOp == pb.Op_Insert
}

func (a *amendCollector) genMutationsForTbl(ctx context.Context, sess *session, commitMutations tikv.CommitterMutations,
	kvMaps map[string][]byte, phyTblID int64, amendOps []amendOp) (tikv.CommitterMutations, tikv.CommitterMutations, error) {
	resAddMutations := tikv.NewCommiterMutations(8)
	resRemoveMutations := tikv.NewCommiterMutations(8)
	for _, curOp := range amendOps {
		curAddMutations, curRemoveMutations, err := curOp.genMutations(ctx, sess, commitMutations, kvMaps)
		if err != nil {
			return resAddMutations, resRemoveMutations, err
		}
		resAddMutations = mergeMutations(resAddMutations, curAddMutations)
		resRemoveMutations = mergeMutations(resRemoveMutations, curRemoveMutations)
	}
	log.Warnf("[for debug] addMutations=%v", resAddMutations)
	return resAddMutations, resRemoveMutations, nil
}

type amendOp interface {
	genMutations(ctx context.Context, sctx sessionctx.Context, commitMutations tikv.CommitterMutations,
		kvMaps map[string][]byte) (tikv.CommitterMutations, tikv.CommitterMutations, error)
}

// amendOperationAddIndex represents one amend operation related to a specific add index change
type amendOperationAddIndex struct {
	dbID        int64
	phyTblID    int64
	idxID       int64
	AmendOpType int

	tblInfoAtStart    *model.TableInfo
	tblInfoAtCommit   *model.TableInfo
	indexInfoAtStart  *model.IndexInfo
	indexInfoAtCommit *model.IndexInfo
	relatedOldIdxCols []*model.ColumnInfo

	decoder *rowcodec.ChunkDecoder
	chk     *chunk.Chunk

	processedNewIndexKeys map[string]interface{}
}

func (a *amendOperationAddIndex) genMutations(ctx context.Context, sctx sessionctx.Context, commitMutations tikv.CommitterMutations,
	kvMaps map[string][]byte) (tikv.CommitterMutations, tikv.CommitterMutations, error) {
	resAddMutations := tikv.NewCommiterMutations(8)
	resRemoveMutations := tikv.NewCommiterMutations(8)
	for i, key := range commitMutations.Keys {
		keyOp := commitMutations.Ops[i]
		addMutations, removeMutations, err := a.processKey(ctx, sctx, keyOp, key, kvMaps)
		if err != nil {
			return resAddMutations, resRemoveMutations, err
		}
		if len(addMutations.Keys) > 0 {
			resAddMutations = mergeMutations(resAddMutations, addMutations)
		}
		if len(removeMutations.Keys) > 0 {
			resRemoveMutations = mergeMutations(resRemoveMutations, removeMutations)
		}
	}
	return resAddMutations, resRemoveMutations, nil
}

func (a *amendOperationAddIndex) genIndexKeyValue(ctx context.Context, sctx sessionctx.Context, kvMaps map[string][]byte,
	key []byte, kvHandle kv.Handle, keyOnly bool) ([]byte, []byte, error) {
	colMap := make(map[int64]*types.FieldType)
	for _, oldCol := range a.tblInfoAtStart.Columns {
		colMap[oldCol.ID] = &oldCol.FieldType
	}
	rowDecoder := a.decoder
	chk := a.chk
	chk.Reset()
	val := kvMaps[string(key)]
	err := rowDecoder.DecodeToChunk(val, kvHandle, chk)
	if err != nil {
		logutil.Logger(ctx).Warn("amend decode value to chunk failed", zap.Int64("tableID", a.phyTblID))
		return nil, nil, err
	}
	idxVals := make([]types.Datum, 0, len(a.indexInfoAtCommit.Columns))
	for _, oldCol := range a.relatedOldIdxCols {
		idxVals = append(idxVals, chk.GetRow(0).GetDatum(oldCol.Offset, &oldCol.FieldType))
	}

	// Generate index key buf
	newIdxKey, distinct, err := tablecodec.GenIndexKey(sctx.GetSessionVars().StmtCtx,
		a.tblInfoAtCommit, a.indexInfoAtCommit, a.phyTblID, idxVals, kvHandle, nil)
	if err != nil {
		logutil.Logger(ctx).Warn("amend generate index key failed", zap.Int64("tableID", a.phyTblID),
			zap.Error(err))
		return nil, nil, err
	}
	if keyOnly {
		return newIdxKey, []byte{}, nil
	}

	// Generate index value buf
	var containsNonBinaryString bool
	for _, idxCol := range a.indexInfoAtCommit.Columns {
		col := a.tblInfoAtCommit.Columns[idxCol.Offset]
		if col.EvalType() == types.ETString && !mysql.HasBinaryFlag(col.Flag) {
			containsNonBinaryString = true
			break
		}
	}
	newIdxVal, err := tablecodec.GenIndexValue(sctx.GetSessionVars().StmtCtx, a.tblInfoAtCommit,
		a.indexInfoAtCommit, containsNonBinaryString, distinct, false, idxVals, kvHandle)
	if err != nil {
		logutil.Logger(ctx).Warn("amend generate index values failed", zap.Int64("tableID", a.phyTblID),
			zap.Error(err))
		return nil, nil, err
	}
	return newIdxKey, newIdxVal, nil
}

func (a *amendOperationAddIndex) processKey(ctx context.Context, sctx sessionctx.Context, keyOp pb.Op, key []byte,
	kvMaps map[string][]byte) (resAdd tikv.CommitterMutations, resRemove tikv.CommitterMutations, err error) {
	if a.AmendOpType == AmendNone {
		return
	}
	kvHandle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		panic("decode row key error")
	}

	// Generated delete index key value
	if (a.AmendOpType == AmendNeedAddDelete || a.AmendOpType == AmendNeedAddDeleteAndInsert) && isDeleteOp(keyOp) {
		newIdxKey, emptyVal, err := a.genIndexKeyValue(ctx, sctx, kvMaps, key, kvHandle, true)
		if err != nil {
			return resAdd, resRemove, err
		}
		resAdd.Push(keyOp, newIdxKey, emptyVal, false)
	}

	// Generated insert index key value
	if (a.AmendOpType == AmendNeedAddDeleteAndInsert || a.AmendOpType == AmendNeedAddInsert) && isInsertOp(keyOp) {
		newIdxKey, newIdxValue, err := a.genIndexKeyValue(ctx, sctx, kvMaps, key, kvHandle, false)
		if err != nil {
			return resAdd, resRemove, err
		}
		if a.indexInfoAtCommit.Unique {
			if _, ok := a.processedNewIndexKeys[string(newIdxKey)]; ok {
				return resAdd, resRemove, errors.Errorf("amend process key same key found")
			}
		}
		a.processedNewIndexKeys[string(newIdxKey)] = nil
		resAdd.Push(keyOp, newIdxKey, newIdxValue, false)
	}
	return
}

func mergeMutations(org tikv.CommitterMutations, delta tikv.CommitterMutations) (newMutations tikv.CommitterMutations) {
	newMutations.Ops = append(org.Ops, delta.Ops...)
	newMutations.Keys = append(org.Keys, delta.Keys...)
	newMutations.Values = append(org.Values, delta.Values...)
	newMutations.IsPessimisticLock = append(org.IsPessimisticLock, delta.IsPessimisticLock...)
	return
}

// SchemaAmenderForTikvTxn is used to amend pessimistic transactions for schema change
type SchemaAmenderForTikvTxn struct {
	sess *session
}

// NewSchemaAmenderForTikvTxn creates a schema amender for tikvTxn type
func NewSchemaAmenderForTikvTxn(sess *session) *SchemaAmenderForTikvTxn {
	amender := &SchemaAmenderForTikvTxn{sess: sess}
	return amender
}

func isWriteCommitOp(keyOp pb.Op) bool {
	if keyOp == pb.Op_Put || keyOp == pb.Op_Del || keyOp == pb.Op_Insert {
		return true
	}
	return false
}

func (s *SchemaAmenderForTikvTxn) getAmendableKeys(commitMutations tikv.CommitterMutations) []kv.Key {
	kvKeys := make([]kv.Key, 0, len(commitMutations.Keys))
	for _, byteKey := range commitMutations.Keys {
		if tablecodec.IsIndexKey(byteKey) {
			continue
		}
		kvKeys = append(kvKeys, byteKey)
	}
	return kvKeys
}

// genAllAmendMutations generates CommitterMutations for all tables and related amend operations
func (s *SchemaAmenderForTikvTxn) genAllAmendMutations(ctx context.Context, commitMutations tikv.CommitterMutations,
	info *amendCollector) (*tikv.CommitterMutations, *tikv.CommitterMutations, error) {
	// Get keys need to be considered for the amend operation, currently only row keys
	kvKeys := s.getAmendableKeys(commitMutations)

	// BatchGet the old row values
	txn, err := s.sess.Txn(true)
	if err != nil {
		return nil, nil, err
	}
	kvMaps, err := txn.BatchGet(ctx, kvKeys)
	if err != nil {
		logutil.Logger(ctx).Warn("amend failed to batch get kv keys", zap.Error(err))
		return nil, nil, err
	}

	// Do generate add/remove mutations processing each key
	addMutations := tikv.NewCommiterMutations(8)
	removeMutations := tikv.NewCommiterMutations(8)
	for phyTblID, amendOps := range info.tblAmendOpMap {
		resAddMutations, resRemoveMutations, err := info.genMutationsForTbl(ctx, s.sess, commitMutations, kvMaps, phyTblID, amendOps)
		if err != nil {
			return nil, nil, err
		}
		addMutations = mergeMutations(addMutations, resAddMutations)
		removeMutations = mergeMutations(removeMutations, resRemoveMutations)
	}
	/*
		for i, key := range commitMutations.Keys {
			if isWriteCommitOp(commitMutations.Ops[i]) {
				resAddMutations, resRemoveMutations, err := info.genMutationsForKey(ctx, s.sess, commitMutations.Ops[i], key, kvMaps)
				if err != nil {
					return nil, nil, err
				}
				addMutations = mergeMutations(addMutations, resAddMutations)
				removeMutations = mergeMutations(removeMutations, resRemoveMutations)
			}
		}
	*/
	return &addMutations, &removeMutations, nil
}

// AmendTxnForSchemaChange does check and generate amend mutations based on input timestamp and mutations,
// the input commitMutations are generated using schema version seen by startTS, change is the related table
// schema changes between startTS and commitTS(or checkTS)
func (s *SchemaAmenderForTikvTxn) AmendTxnForSchemaChange(ctx context.Context, startTS uint64, commitTs uint64,
	change *tikv.RelatedSchemaChange, commitMutations tikv.CommitterMutations) (
	*tikv.CommitterMutations, *tikv.CommitterMutations, error) {
	// Get schema meta for startTS and commitTS
	snapStartTS, err := s.sess.GetStore().GetSnapshot(kv.Version{Ver: startTS})
	if err != nil {
		return nil, nil, err
	}
	startTSMeta := meta.NewSnapshotMeta(snapStartTS)
	snapCommitTS, err := s.sess.GetStore().GetSnapshot(kv.Version{Ver: commitTs})
	if err != nil {
		return nil, nil, err
	}
	commitTSMeta := meta.NewSnapshotMeta(snapCommitTS)

	// Generate amend operations for each table by physical table id
	var needAmendMem bool
	amendCollector := newAmendCollector()
	for i, tblID := range change.PhyTblIDS {
		dbID := change.DBIDs[i]
		actionType := change.ActionTypes[i]
		// Check amendable flags, return if not supported flags exist
		if actionType&(^amendableType) != 0 {
			return nil, nil, table.ErrUnsupportedOp
		}
		// Partition table is not supported now
		tblInfoAtStart, err := startTSMeta.GetTable(dbID, tblID)
		if err != nil {
			return nil, nil, err
		}
		if tblInfoAtStart.Partition != nil {
			logutil.Logger(ctx).Info("Amend for partition table is not supported", zap.Uint64("startTS", startTS),
				zap.Int64("tableID", tblID))
			return nil, nil, table.ErrUnsupportedOp
		}
		tblInfoAtCommit, err := commitTSMeta.GetTable(dbID, tblID)
		if err != nil {
			return nil, nil, err
		}
		if actionType&(memBufAmendType) != 0 {
			needAmendMem = true
			err := amendCollector.collectTblAmendOps(ctx, s.sess, dbID, tblID, tblInfoAtStart, tblInfoAtCommit)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if needAmendMem {
		for tblID, ops := range amendCollector.tblAmendOpMap {
			for _, op := range ops {
				log.Warnf("tblOD=%v ops=%v", tblID, op)
			}
		}
		return s.genAllAmendMutations(ctx, commitMutations, amendCollector)
	}
	return nil, nil, nil
}

func newRowChkDecoder(ctx sessionctx.Context, tbl *model.TableInfo) *rowcodec.ChunkDecoder {
	getColInfoByID := func(tbl *model.TableInfo, colID int64) *model.ColumnInfo {
		for _, col := range tbl.Columns {
			if col.ID == colID {
				return col
			}
		}
		return nil
	}
	var pkCols []int64
	reqCols := make([]rowcodec.ColInfo, len(tbl.Columns))
	for i := range tbl.Columns {
		idx, col := i, tbl.Columns[i]
		isPK := (tbl.PKIsHandle && mysql.HasPriKeyFlag(col.FieldType.Flag)) || col.ID == model.ExtraHandleID
		if isPK {
			pkCols = append(pkCols, col.ID)
		}

		isVirtualGenCol := col.IsGenerated() && !col.GeneratedStored
		reqCols[idx] = rowcodec.ColInfo{
			ID:            col.ID,
			VirtualGenCol: isVirtualGenCol,
			Ft:            &col.FieldType,
		}
	}
	if len(pkCols) == 0 {
		pkCols = tables.TryGetCommonPkColumnIds(tbl)
		if len(pkCols) == 0 {
			pkCols = []int64{0}
		}
	}
	defVal := func(i int, chk *chunk.Chunk) error {
		ci := getColInfoByID(tbl, reqCols[i].ID)
		d, err := table.GetColOriginDefaultValue(ctx, ci)
		if err != nil {
			return err
		}
		chk.AppendDatum(i, &d)
		return nil
	}
	return rowcodec.NewChunkDecoder(reqCols, pkCols, defVal, ctx.GetSessionVars().TimeZone)
}
