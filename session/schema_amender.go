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
	"go.uber.org/zap"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
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

// amendOperations has all amend operations for each related table having schema changes
type amendOperations struct {
	tblMap map[int64][]amendOperation
}

func newAmendOperations() *amendOperations {
	res := &amendOperations{
		tblMap: make(map[int64][]amendOperation),
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
		// Try to find index with "add index" state change
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
				// TODO add index on generated column is not supported by now
				if oldColInfo.IsGenerated() {
					logutil.Logger(ctx).Warn("generated column is not supported in amend")
					return nil, table.ErrUnsupportedOp
				}
			}
			res = append(res, op)
		}
	}
	return res, nil
}

// genTableAmendOps generates amend operations for each table with related schema change
func (a *amendOperations) genTableAmendOps(ctx context.Context, dbID, phyTblID int64, tblInfoAtStart, tblInfoAtCommit *model.TableInfo) error {
	// TODO generate needed amend ops
	// Currently only add index is considered
	ops, err := a.genAddIndexAmendOps(ctx, dbID, phyTblID, tblInfoAtStart, tblInfoAtCommit)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[for debug]genTableAmendOps ops=%v", ops)
	if _, ok := a.tblMap[phyTblID]; !ok {
		a.tblMap[phyTblID] = make([]amendOperation, 0, 4)
	}
	a.tblMap[phyTblID] = append(a.tblMap[phyTblID], ops...)
	return nil
}

func isDeleteOp(keyOp pb.Op) bool {
	return keyOp == pb.Op_Del
}

func isInsertOp(keyOp pb.Op) bool {
	return keyOp == pb.Op_Put || keyOp == pb.Op_Insert
}

func (a *amendOperations) genOneMut(sess *session, amendOp *amendOperation, kvMaps map[string][]byte, key []byte,
	kvHandle kv.Handle) ([]byte, []byte, error) {
	idxPrefix := tablecodec.EncodeTableIndexPrefix(amendOp.phyTblID, amendOp.idxID)
	colMap := make(map[int64]*types.FieldType)
	for _, oldCol := range amendOp.tblInfoAtStart.Columns {
		colMap[oldCol.ID] = &oldCol.FieldType
	}
	rowMap, err := tablecodec.DecodeRow(kvMaps[string(key)], colMap, time.UTC)
	if err != nil {
		panic("decode err")
	}
	// Debug test code
	log.Warnf("[for debug] rowMap=%v", rowMap)
	for k, v := range rowMap {
		log.Warnf("[for debug] decoded k=%v, v=%v, idxPrefix=%v", k, v, idxPrefix)
	}
	idxVals := make([]types.Datum, 0, len(amendOp.indexInfoAtCommit.Columns))
	for _, oldCol := range amendOp.relatedOldIdxCols {
		idxVals = append(idxVals, rowMap[oldCol.ID])
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
	log.Warnf("newIdxVal=%v", newIdxVal)
	return newIdxKey, newIdxVal, nil
}

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
	if amendOpArr, ok := a.tblMap[phyTblID]; ok {
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
				newIdxKey, newIdxVal, err := a.genOneMut(sess, &amendOp, kvMaps, key, kvHandle)
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

func (s *SchemaAmenderForTikvTxn) genMemAmendMutations(ctx context.Context, commitMutations tikv.CommitterMutations,
	info *amendOperations) (*tikv.CommitterMutations, *tikv.CommitterMutations, error) {
	kvKeys := make([]kv.Key, 0, len(commitMutations.Keys))
	for _, byteKey := range commitMutations.Keys {
		if tablecodec.IsIndexKey(byteKey) {
			continue
		}
		kvKeys = append(kvKeys, byteKey)
	}
	// BatchGet the old row values
	txn, err := s.sess.Txn(true)
	if err != nil {
		return nil, nil, err
	}
	kvMaps, err := txn.BatchGet(ctx, kvKeys)
	if err != nil {
		panic(err)
	}
	addMutations := tikv.NewCommiterMutations(8)
	removeMutations := tikv.NewCommiterMutations(8)
	for i, key := range commitMutations.Keys {
		if isWriteCommitOp(commitMutations.Ops[i]) {
			resAddMuts, resRemoveMuts, err := info.genMutations(ctx, s.sess, commitMutations.Ops[i], key, kvMaps)
			if err != nil {
				return nil, nil, err
			}
			addMutations = mergeMutations(addMutations, resAddMuts)
			removeMutations = mergeMutations(removeMutations, resRemoveMuts)
		}
	}
	return &addMutations, &removeMutations, nil
}

// AmendTxnForSchemaChange generates amend CommiterMutations for the committing transaction
func (s *SchemaAmenderForTikvTxn) AmendTxnForSchemaChange(ctx context.Context, startTS uint64, commitTs uint64,
	change *tikv.RelatedSchemaChange, commitMutations tikv.CommitterMutations) (
	*tikv.CommitterMutations, *tikv.CommitterMutations, error) {
	// Get schema meta for startTS and commitTS
	startTSVer := kv.Version{Ver: startTS}
	snapStartTS, err := s.sess.GetStore().GetSnapshot(startTSVer)
	if err != nil {
		return nil, nil, err
	}
	startTSMeta := meta.NewSnapshotMeta(snapStartTS)
	commitTSVer := kv.Version{Ver: commitTs}
	snapCommitTS, err := s.sess.GetStore().GetSnapshot(commitTSVer)
	if err != nil {
		return nil, nil, err
	}
	commitTSMeta := meta.NewSnapshotMeta(snapCommitTS)

	// Generate amend operations for each table
	var needAmendMem bool
	amendOps := newAmendOperations()
	for i, tblID := range change.PhyTblIDS {
		// Check amendable flags, return if not supported flags exist
		if change.ActionTypes[i]&(^amendableType) != 0 {
			return nil, nil, table.ErrUnsupportedOp
		}
		// Partition table is not supported now
		tblInfoAtStart, err := startTSMeta.GetTable(change.DBIDs[i], tblID)
		if err != nil {
			return nil, nil, err
		}
		if tblInfoAtStart.Partition != nil {
			logutil.Logger(ctx).Info("Unsupported amend found, partition table", zap.Int64("tableID", tblID),
				zap.Uint64("actionType", change.ActionTypes[i]))
			return nil, nil, table.ErrUnsupportedOp
		}
		tblInfoAtCommit, err := commitTSMeta.GetTable(change.DBIDs[i], tblID)
		if err != nil {
			return nil, nil, err
		}
		if change.ActionTypes[i]&(memBufAmendType) != 0 {
			needAmendMem = true
			err := amendOps.genTableAmendOps(ctx, change.DBIDs[i], tblID, tblInfoAtStart, tblInfoAtCommit)
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
