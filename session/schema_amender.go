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
	"encoding/hex"
	"fmt"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/zap"
)

const amendableType = nonMemAmendType | (memBufAmendType)
const nonMemAmendType = (1 << model.ActionAddColumn) | (1 << model.ActionDropColumn) | (1 << model.ActionDropIndex)
const memBufAmendType = uint64(1 << model.ActionAddIndex)

// Amend operation types.
const (
	AmendNone int = iota

	// For add index.
	AmendNeedAddDelete
	AmendNeedAddDeleteAndInsert
	AmendNeedAddInsert
)

// ConstOpAddIndex is the possible ddl state changes, and related amend action types.
var ConstOpAddIndex = map[model.SchemaState]map[model.SchemaState]int{
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

type schemaAndDecoder struct {
	schema  *expression.Schema
	decoder *rowcodec.ChunkDecoder
}

// amendCollector collects all amend operations, row decoders and memory chunks for each table needs amend.
type amendCollector struct {
	tblAmendOpMap map[int64][]amendOp
	tblDecoder    map[int64]*schemaAndDecoder
	tblChk        map[int64]*chunk.Chunk
}

func newAmendCollector() *amendCollector {
	res := &amendCollector{
		tblAmendOpMap: make(map[int64][]amendOp),
		tblDecoder:    make(map[int64]*schemaAndDecoder),
		tblChk:        make(map[int64]*chunk.Chunk),
	}
	return res
}

func findIndexByID(tbl table.Table, ID int64) table.Index {
	for _, indexInfo := range tbl.Indices() {
		if indexInfo.Meta().ID == ID {
			return indexInfo
		}
	}
	return nil
}

func findColByID(tbl table.Table, colID int64) *table.Column {
	for _, colInfo := range tbl.Cols() {
		if colInfo.ID == colID {
			return colInfo
		}
	}
	return nil
}

func (a *amendCollector) keyHasAmendOp(key []byte) bool {
	tblID := tablecodec.DecodeTableID(key)
	ops := a.tblAmendOpMap[tblID]
	return len(ops) > 0
}

func (a *amendCollector) collectIndexAmendOps(phyTblID int64, tblAtStart, tblAtCommit table.Table) ([]amendOp, error) {
	res := make([]amendOp, 0, 4)
	// Check index having state change, collect index column info.
	for _, idxInfoAtCommit := range tblAtCommit.Indices() {
		idxInfoAtStart := findIndexByID(tblAtStart, idxInfoAtCommit.Meta().ID)
		// Try to find index state change.
		var amendOpType int
		if idxInfoAtStart == nil {
			amendOpType = ConstOpAddIndex[model.StateNone][idxInfoAtCommit.Meta().State]
		} else if idxInfoAtCommit.Meta().State > idxInfoAtStart.Meta().State {
			amendOpType = ConstOpAddIndex[idxInfoAtStart.Meta().State][idxInfoAtCommit.Meta().State]
		}
		if amendOpType != AmendNone {
			op := amendOperationAddIndex{}
			op.AmendOpType = amendOpType
			op.tblInfoAtStart = tblAtStart
			op.tblInfoAtCommit = tblAtCommit
			op.indexInfoAtStart = idxInfoAtStart
			op.indexInfoAtCommit = idxInfoAtCommit
			for _, idxCol := range idxInfoAtCommit.Meta().Columns {
				colID := tblAtCommit.Meta().Columns[idxCol.Offset].ID
				oldColInfo := findColByID(tblAtStart, colID)
				// TODO: now index column MUST be found in old table columns, generated column is not supported.
				if oldColInfo == nil || oldColInfo.IsGenerated() {
					return nil, errors.Trace(table.ErrUnsupportedOp)
				}
				op.relatedOldIdxCols = append(op.relatedOldIdxCols, oldColInfo)
			}

			op.schemaAndDecoder = a.tblDecoder[phyTblID]
			op.chk = a.tblChk[phyTblID]
			op.processedNewIndexKeys = make(map[string]interface{})
			res = append(res, &op)
		}
	}
	return res, nil
}

// collectTblAmendOps collects amend operations for each table using the schema diff between startTS and commitTS.
func (a *amendCollector) collectTblAmendOps(sctx sessionctx.Context, phyTblID int64,
	tblInfoAtStart, tblInfoAtCommit table.Table) error {
	if _, ok := a.tblAmendOpMap[phyTblID]; !ok {
		a.tblAmendOpMap[phyTblID] = make([]amendOp, 0, 4)
		a.tblDecoder[phyTblID] = newSchemaAndDecoder(sctx, tblInfoAtStart.Meta())
		fieldTypes := make([]*types.FieldType, 0, len(tblInfoAtStart.Meta().Columns))
		for _, col := range tblInfoAtStart.Meta().Columns {
			fieldTypes = append(fieldTypes, &col.FieldType)
		}
		a.tblChk[phyTblID] = chunk.NewChunkWithCapacity(fieldTypes, 4)
	}
	// TODO: currently only "add index" is considered.
	ops, err := a.collectIndexAmendOps(phyTblID, tblInfoAtStart, tblInfoAtCommit)
	if err != nil {
		return err
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
	newValKvMap map[string][]byte, oldValKvMap map[string][]byte, amendOps []amendOp) (tikv.CommitterMutations, error) {
	resAddMutations := tikv.NewCommiterMutations(8)
	for _, curOp := range amendOps {
		curAddMutations, err := curOp.genMutations(ctx, sess, commitMutations, newValKvMap, oldValKvMap)
		if err != nil {
			return resAddMutations, err
		}
		resAddMutations.MergeMutations(curAddMutations)
	}
	return resAddMutations, nil
}

// amendOp is an amend operation for a specific schema change, new mutations will be generated using input ones.
type amendOp interface {
	genMutations(ctx context.Context, sctx sessionctx.Context, commitMutations tikv.CommitterMutations,
		newValKvMap map[string][]byte, oldValKvMap map[string][]byte) (tikv.CommitterMutations, error)
}

// amendOperationAddIndex represents one amend operation related to a specific add index change.
type amendOperationAddIndex struct {
	AmendOpType       int
	tblInfoAtStart    table.Table
	tblInfoAtCommit   table.Table
	indexInfoAtStart  table.Index
	indexInfoAtCommit table.Index
	relatedOldIdxCols []*table.Column

	schemaAndDecoder      *schemaAndDecoder
	chk                   *chunk.Chunk
	processedNewIndexKeys map[string]interface{}
}

func (a *amendOperationAddIndex) String() string {
	var colStr string
	colStr += "["
	for _, colInfo := range a.relatedOldIdxCols {
		colStr += fmt.Sprintf(" %s ", colInfo.Name)
	}
	colStr += "]"
	res := fmt.Sprintf("AmenedOpType=%d phyTblID=%d idxID=%d columns=%v", a.AmendOpType, a.indexInfoAtCommit.Meta().ID,
		a.indexInfoAtCommit.Meta().ID, colStr)
	return res
}

func (a *amendOperationAddIndex) genMutations(ctx context.Context, sctx sessionctx.Context, commitMutations tikv.CommitterMutations,
	newValKvMap map[string][]byte, oldValKvMap map[string][]byte) (tikv.CommitterMutations, error) {
	resAddMutations := tikv.NewCommiterMutations(8)
	for i, key := range commitMutations.GetKeys() {
		keyOp := commitMutations.GetOps()[i]
		if tablecodec.IsIndexKey(key) || tablecodec.DecodeTableID(key) != a.tblInfoAtCommit.Meta().ID {
			continue
		}
		addMutations, err := a.processKey(ctx, sctx, keyOp, key, newValKvMap, oldValKvMap)
		if err != nil {
			return resAddMutations, err
		}
		if len(addMutations.GetKeys()) > 0 {
			resAddMutations.MergeMutations(addMutations)
		}
	}
	return resAddMutations, nil
}

func (a *amendOperationAddIndex) genIndexKeyValue(ctx context.Context, sctx sessionctx.Context, kvMaps map[string][]byte,
	key []byte, kvHandle kv.Handle, keyOnly bool) ([]byte, []byte, error) {
	colMap := make(map[int64]*types.FieldType)
	for _, oldCol := range a.tblInfoAtStart.Meta().Cols() {
		colMap[oldCol.ID] = &oldCol.FieldType
	}
	chk := a.chk
	chk.Reset()
	val := kvMaps[string(key)]
	err := executor.DecodeRowValToChunk(sctx, a.schemaAndDecoder.schema, a.tblInfoAtStart.Meta(), kvHandle, val, chk, a.schemaAndDecoder.decoder)
	if err != nil {
		logutil.Logger(ctx).Warn("amend decode value to chunk failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	idxVals := make([]types.Datum, 0, len(a.indexInfoAtCommit.Meta().Columns))
	for _, oldCol := range a.relatedOldIdxCols {
		idxVals = append(idxVals, chk.GetRow(0).GetDatum(oldCol.Offset, &oldCol.FieldType))
	}

	// Generate index key buf.
	newIdxKey, distinct, err := tablecodec.GenIndexKey(sctx.GetSessionVars().StmtCtx,
		a.tblInfoAtCommit.Meta(), a.indexInfoAtCommit.Meta(), a.tblInfoAtCommit.Meta().ID, idxVals, kvHandle, nil)
	if err != nil {
		logutil.Logger(ctx).Warn("amend generate index key failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	if keyOnly {
		return newIdxKey, []byte{}, nil
	}

	// Generate index value buf.
	var containsNonBinaryString bool
	for _, idxCol := range a.indexInfoAtCommit.Meta().Columns {
		col := a.tblInfoAtCommit.Meta().Columns[idxCol.Offset]
		if col.EvalType() == types.ETString && !mysql.HasBinaryFlag(col.Flag) {
			containsNonBinaryString = true
			break
		}
	}
	newIdxVal, err := tablecodec.GenIndexValue(sctx.GetSessionVars().StmtCtx, a.tblInfoAtCommit.Meta(),
		a.indexInfoAtCommit.Meta(), containsNonBinaryString, distinct, false, idxVals, kvHandle)
	if err != nil {
		logutil.Logger(ctx).Warn("amend generate index values failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	return newIdxKey, newIdxVal, nil
}

func (a *amendOperationAddIndex) processKey(ctx context.Context, sctx sessionctx.Context, keyOp pb.Op, key []byte,
	newValKvMap map[string][]byte, oldValKvMap map[string][]byte) (resAdd tikv.CommitterMutations, err error) {
	kvHandle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		logutil.Logger(ctx).Error("decode key error", zap.String("key", hex.EncodeToString(key)), zap.Error(err))
		return resAdd, errors.Trace(err)
	}

	// Generated delete index key value.
	if (a.AmendOpType == AmendNeedAddDelete || a.AmendOpType == AmendNeedAddDeleteAndInsert) && isDeleteOp(keyOp) {
		newIdxKey, emptyVal, err := a.genIndexKeyValue(ctx, sctx, oldValKvMap, key, kvHandle, true)
		if err != nil {
			return resAdd, errors.Trace(err)
		}
		resAdd.Push(keyOp, newIdxKey, emptyVal, false)
	}

	// Generated insert index key value.
	if (a.AmendOpType == AmendNeedAddDeleteAndInsert || a.AmendOpType == AmendNeedAddInsert) && isInsertOp(keyOp) {
		newIdxKey, newIdxValue, err := a.genIndexKeyValue(ctx, sctx, newValKvMap, key, kvHandle, false)
		if err != nil {
			return resAdd, errors.Trace(err)
		}
		// Check if the generated index keys are unique for unique index.
		if a.indexInfoAtCommit.Meta().Unique {
			if _, ok := a.processedNewIndexKeys[string(newIdxKey)]; ok {
				return resAdd, errors.Trace(errors.Errorf("amend process key same key=%v found for unique index=%v",
					newIdxKey, a.indexInfoAtCommit.Meta().Name))
			}
		}
		a.processedNewIndexKeys[string(newIdxKey)] = nil
		resAdd.Push(keyOp, newIdxKey, newIdxValue, false)
	}
	return
}

// SchemaAmender is used to amend pessimistic transactions for schema change.
type SchemaAmender struct {
	sess *session
}

// NewSchemaAmenderForTikvTxn creates a schema amender for tikvTxn type.
func NewSchemaAmenderForTikvTxn(sess *session) *SchemaAmender {
	amender := &SchemaAmender{sess: sess}
	return amender
}

func (s *SchemaAmender) getAmendableKeys(commitMutations tikv.CommitterMutations, info *amendCollector) ([]kv.Key, []kv.Key) {
	addKeys := make([]kv.Key, 0, len(commitMutations.GetKeys()))
	removeKeys := make([]kv.Key, 0, len(commitMutations.GetKeys()))
	for i, byteKey := range commitMutations.GetKeys() {
		if tablecodec.IsIndexKey(byteKey) || !info.keyHasAmendOp(byteKey) {
			continue
		}
		if isInsertOp(commitMutations.GetOps()[i]) {
			addKeys = append(addKeys, byteKey)
		} else if isDeleteOp(commitMutations.GetOps()[i]) {
			removeKeys = append(removeKeys, byteKey)
		} else {
			// Do nothing.
		}
	}
	return addKeys, removeKeys
}

// genAllAmendMutations generates CommitterMutations for all tables and related amend operations.
func (s *SchemaAmender) genAllAmendMutations(ctx context.Context, commitMutations tikv.CommitterMutations,
	info *amendCollector) (*tikv.CommitterMutations, error) {
	// Get keys need to be considered for the amend operation, currently only row keys.
	addKeys, removeKeys := s.getAmendableKeys(commitMutations, info)

	// BatchGet the key values.
	txn, err := s.sess.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newValKvMap, err := txn.BatchGet(ctx, addKeys)
	if err != nil {
		logutil.Logger(ctx).Warn("amend failed to batch get kv new keys", zap.Error(err))
		return nil, errors.Trace(err)
	}
	oldValKvMap, err := txn.GetSnapshot().BatchGet(ctx, removeKeys)
	if err != nil {
		logutil.Logger(ctx).Warn("amend failed to batch get kv old keys", zap.Error(err))
		return nil, errors.Trace(err)
	}

	// Do generate add/remove mutations processing each key.
	addMutations := tikv.NewCommiterMutations(8)
	for _, amendOps := range info.tblAmendOpMap {
		resAddMutations, err := info.genMutationsForTbl(ctx, s.sess, commitMutations, newValKvMap, oldValKvMap, amendOps)
		if err != nil {
			return nil, err
		}
		addMutations.MergeMutations(resAddMutations)
	}
	return &addMutations, nil
}

// AmendTxn does check and generate amend mutations based on input infoSchema and mutations, mutations need to prewrite
// are returned, the input commitMutations will not be changed.
func (s *SchemaAmender) AmendTxn(ctx context.Context, startInfoSchema tikv.SchemaVer, change *tikv.RelatedSchemaChange,
	commitMutations tikv.CommitterMutations) (*tikv.CommitterMutations, error) {
	// Get info schema meta
	infoSchemaAtStart := startInfoSchema.(infoschema.InfoSchema)
	infoSchemaAtCheck := change.LatestInfoSchema.(infoschema.InfoSchema)

	// Collect amend operations for each table by physical table ID.
	var needAmendMem bool
	amendCollector := newAmendCollector()
	for i, tblID := range change.PhyTblIDS {
		actionType := change.ActionTypes[i]
		// Check amendable flags, return if not supported flags exist.
		if actionType&(^amendableType) != 0 {
			logutil.Logger(ctx).Info("amend action type not supported for txn", zap.Int64("tblID", tblID), zap.Uint64("actionType", actionType))
			return nil, errors.Trace(table.ErrUnsupportedOp)
		}
		// Partition table is not supported now.
		tblInfoAtStart, ok := infoSchemaAtStart.TableByID(tblID)
		if !ok {
			return nil, errors.Trace(errors.Errorf("tableID=%d is not found in infoSchema", tblID))
		}
		if tblInfoAtStart.Meta().Partition != nil {
			logutil.Logger(ctx).Info("Amend for partition table is not supported", zap.Int64("tableID", tblID))
			return nil, errors.Trace(table.ErrUnsupportedOp)
		}
		tblInfoAtCommit, ok := infoSchemaAtCheck.TableByID(tblID)
		if !ok {
			return nil, errors.Trace(errors.Errorf("tableID=%d is not found in infoSchema", tblID))
		}
		if actionType&(memBufAmendType) != 0 {
			needAmendMem = true
			err := amendCollector.collectTblAmendOps(s.sess, tblID, tblInfoAtStart, tblInfoAtCommit)
			if err != nil {
				return nil, err
			}
		}
	}
	// After amend operations collect, generate related new mutations based on input commitMutations
	if needAmendMem {
		return s.genAllAmendMutations(ctx, commitMutations, amendCollector)
	}
	return nil, nil
}

func newSchemaAndDecoder(ctx sessionctx.Context, tbl *model.TableInfo) *schemaAndDecoder {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(tbl.Columns))...)
	for _, col := range tbl.Columns {
		colExpr := &expression.Column{
			RetType: &col.FieldType,
			ID:      col.ID,
		}
		if col.IsGenerated() && !col.GeneratedStored {
			// This will not be used since generated column is rejected in collectIndexAmendOps.
			colExpr.VirtualExpr = &expression.Constant{}
		}
		schema.Append(colExpr)
	}
	return &schemaAndDecoder{schema, executor.NewRowDecoder(ctx, schema, tbl)}
}
