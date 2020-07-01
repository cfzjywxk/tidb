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
	"time"
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

// amendOperations has all amend operations, row decoders and memory chunks for tables need amend
type amendOperations struct {
	tblAmendOpMap map[int64][]amendOperation
	tblDecoder    map[int64]*rowcodec.ChunkDecoder
	tblChk        map[int64]*chunk.Chunk
}

func newAmendOperations() *amendOperations {
	res := &amendOperations{
		tblAmendOpMap: make(map[int64][]amendOperation),
		tblDecoder:    make(map[int64]*rowcodec.ChunkDecoder),
		tblChk:        make(map[int64]*chunk.Chunk),
	}
	return res
}

func (a *amendOperations) genAddIndexAmendOps(ctx context.Context, dbID, phyTblID int64,
	tblAtStart, tblAtCommit *model.TableInfo) ([]amendOperation, error) {
	res := make([]amendOperation, 0, 4)
	op := amendOperation{}
	op.dbID = dbID
	op.phyTblID = phyTblID
	op.tblInfoAtStart = tblAtStart
	op.tblInfoAtCommit = tblAtCommit
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
					log.Warnf("column=%v not found in original schema", *idxCol)
					return nil, table.ErrUnsupportedOp
				}
			}
			res = append(res, op)
		}
	}
	return res, nil
}

// genTableAmendOps generates amend operations for each table
func (a *amendOperations) genTableAmendOps(ctx context.Context, sctx sessionctx.Context, dbID, phyTblID int64,
	tblInfoAtStart, tblInfoAtCommit *model.TableInfo) error {
	// TODO currently only add index is considered
	ops, err := a.genAddIndexAmendOps(ctx, dbID, phyTblID, tblInfoAtStart, tblInfoAtCommit)
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := a.tblAmendOpMap[phyTblID]; !ok {
		a.tblAmendOpMap[phyTblID] = make([]amendOperation, 0, 4)
		a.tblDecoder[phyTblID] = newRowChkDecoder(sctx, tblInfoAtStart)
		fieldTypes := make([]*types.FieldType, 0, len(tblInfoAtStart.Columns))
		for _, col := range tblInfoAtStart.Columns {
			fieldTypes = append(fieldTypes, &col.FieldType)
		}
		a.tblChk[phyTblID] = chunk.NewChunkWithCapacity(fieldTypes, 4)
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

func (a *amendOperations) genNewIndexKV(sess *session, amendOp *amendOperation, kvMaps map[string][]byte, key []byte,
	kvHandle kv.Handle, keyOnly bool) ([]byte, []byte, error) {
	idxPrefix := tablecodec.EncodeTableIndexPrefix(amendOp.phyTblID, amendOp.idxID)
	colMap := make(map[int64]*types.FieldType)
	for _, oldCol := range amendOp.tblInfoAtStart.Columns {
		colMap[oldCol.ID] = &oldCol.FieldType
	}
	rowMap, err := tablecodec.DecodeRow(kvMaps[string(key)], colMap, time.UTC)
	if err != nil {
		panic("decode err")
	}
	rowDecoder := a.tblDecoder[amendOp.phyTblID]
	chk := a.tblChk[amendOp.phyTblID]
	val := kvMaps[string(key)]
	err = rowDecoder.DecodeToChunk(val, kvHandle, chk)
	if err != nil {
		panic("decode err")
	}
	idxVals := make([]types.Datum, 0, len(amendOp.indexInfoAtCommit.Columns))
	for _, oldCol := range amendOp.relatedOldIdxCols {
		//idxVals = append(idxVals, rowMap[oldCol.ID])
		log.Warnf("[for debug] rowMap_value=%v chk_value=%v", rowMap[oldCol.ID], chk.GetRow(0).GetDatum(oldCol.Offset, &oldCol.FieldType))
		idxVals = append(idxVals, chk.GetRow(0).GetDatum(oldCol.Offset, &oldCol.FieldType))
	}

	// Generate index key buf
	newIdxKey, distinct, err := tablecodec.GenIndexKeyUsingPrefix(sess.GetSessionVars().StmtCtx,
		amendOp.tblInfoAtCommit, amendOp.indexInfoAtCommit, idxPrefix, idxVals, kvHandle, nil)
	if err != nil {
		panic(err)
	}
	log.Warnf("newIdxKey=%v", newIdxKey)

	// Generate index value buf
	var containsNonBinaryString bool
	for _, idxCol := range amendOp.indexInfoAtCommit.Columns {
		col := amendOp.tblInfoAtCommit.Columns[idxCol.Offset]
		if col.EvalType() == types.ETString && !mysql.HasBinaryFlag(col.Flag) {
			containsNonBinaryString = true
			break
		}
	}
	newIdxVal, err := tablecodec.GenIndexValue(sess.GetSessionVars().StmtCtx, amendOp.tblInfoAtCommit,
		amendOp.indexInfoAtCommit, containsNonBinaryString, distinct, false, idxVals, kvHandle)
	if err != nil {
		panic(err)
	}
	return newIdxKey, newIdxVal, nil
}

// genMutations generates all mutations needed for the input key
func (a *amendOperations) genMutations(ctx context.Context, sess *session, KeyOp pb.Op, key []byte,
	kvMaps map[string][]byte) (tikv.CommitterMutations, tikv.CommitterMutations, error) {
	var resNewMutations tikv.CommitterMutations
	var resOldMutations tikv.CommitterMutations
	phyTblID := tablecodec.DecodeTableID(key)
	if phyTblID == 0 {
		return resNewMutations, resOldMutations, errors.Errorf("decode key=%x table id results in zero", key)
	}
	kvHandle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		panic("decode row key error")
	}
	if amendOpArr, ok := a.tblAmendOpMap[phyTblID]; ok {
		log.Warnf("[for debug] processing key=%x table ops=%v", key, amendOpArr)
		for _, amendOp := range amendOpArr {
			if amendOp.AmendOpType == AmendNone {
				continue
			}
			switch amendOp.AmendOpType {
			case AmendNone:
			case AmendNeedAddDelete:
				if isInsertOp(KeyOp) {
					continue
				}
				fallthrough
			case AmendNeedAddDeleteAndInsert:
				fallthrough
			case AmendNeedAddInsert:
				if isDeleteOp(KeyOp) {
					continue
				}
				newIdxKey, newIdxVal, err := a.genNewIndexKV(sess, &amendOp, kvMaps, key, kvHandle, false)
				if err != nil {
					panic(err)
				}
				resNewMutations.Ops = append(resNewMutations.Ops, KeyOp)
				resNewMutations.Keys = append(resNewMutations.Keys, newIdxKey)
				resNewMutations.Values = append(resNewMutations.Values, newIdxVal)
				resNewMutations.IsPessimisticLock = append(resNewMutations.IsPessimisticLock, false)
			case AmendNeedRemoveInsert:
				return resNewMutations, resOldMutations, table.ErrUnsupportedOp
			case AmendNeedRemoveInsertAndDelete:
				return resNewMutations, resOldMutations, table.ErrUnsupportedOp
			case AmendNeedRemoveDelete:
				return resNewMutations, resOldMutations, table.ErrUnsupportedOp
			}
		}
	}
	return resNewMutations, resOldMutations, nil
}

// amendOperation represents one amend operation related to a specific index
type amendOperation struct {
	dbID              int64
	phyTblID          int64
	idxID             int64
	AmendOpType       int
	tblInfoAtStart    *model.TableInfo
	tblInfoAtCommit   *model.TableInfo
	indexInfoAtStart  *model.IndexInfo
	indexInfoAtCommit *model.IndexInfo
	relatedOldIdxCols []*model.ColumnInfo
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

func (s *SchemaAmenderForTikvTxn) genMemAmendMutations(ctx context.Context, commitMutations tikv.CommitterMutations,
	info *amendOperations) (*tikv.CommitterMutations, *tikv.CommitterMutations, error) {
	// Get keys need to be considered for the amend operation, currently only row keys
	kvKeys := s.getAmendableKeys(commitMutations)

	// BatchGet the old row values
	txn, err := s.sess.Txn(true)
	if err != nil {
		return nil, nil, err
	}
	kvMaps, err := txn.BatchGet(ctx, kvKeys)
	if err != nil {
		panic(err)
	}

	// Do generate mutations
	addMutations := tikv.NewCommiterMutations(8)
	removeMutations := tikv.NewCommiterMutations(8)
	for i, key := range commitMutations.Keys {
		if isWriteCommitOp(commitMutations.Ops[i]) {
			resAddMutations, resRemoveMutations, err := info.genMutations(ctx, s.sess, commitMutations.Ops[i], key, kvMaps)
			if err != nil {
				return nil, nil, err
			}
			addMutations = mergeMutations(addMutations, resAddMutations)
			removeMutations = mergeMutations(removeMutations, resRemoveMutations)
		}
	}
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
	amendOps := newAmendOperations()
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
			logutil.Logger(ctx).Info("Amend for partition table is not supported", zap.Uint64("startTS", startTS), zap.Int64("tableID", tblID))
			return nil, nil, table.ErrUnsupportedOp
		}
		tblInfoAtCommit, err := commitTSMeta.GetTable(dbID, tblID)
		if err != nil {
			return nil, nil, err
		}
		if actionType&(memBufAmendType) != 0 {
			needAmendMem = true
			err := amendOps.genTableAmendOps(ctx, s.sess, dbID, tblID, tblInfoAtStart, tblInfoAtCommit)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if needAmendMem {
		return s.genMemAmendMutations(ctx, commitMutations, amendOps)
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
