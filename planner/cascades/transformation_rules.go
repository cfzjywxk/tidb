// Copyright 2018 PingCAP, Inc.
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

package cascades

import (
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/util/ranger"
)

// Transformation defines the interface for the transformation rules.
type Transformation interface {
	GetPattern() *memo.Pattern
	// Match is used to check whether the GroupExpr satisfies all the requirements of the transformation rule.
	//
	// The pattern only identifies the operator type, some transformation rules also need
	// detailed information for certain plan operators to decide whether it is applicable.
	Match(expr *memo.ExprIter) bool
	// OnTransform does the real work of the optimization rule.
	//
	// newExprs indicates the new GroupExprs generated by the transformationrule. Multiple GroupExprs may be
	// returned, e.g, EnumeratePath would convert DataSource to several possible assess paths.
	//
	// eraseOld indicates that the returned GroupExpr must be better than the old one, so we can remove it from Group.
	//
	// eraseAll indicates that the returned GroupExpr must be better than all other candidates in the Group, e.g, we can
	// prune all other access paths if we found the filter is constantly false.
	OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error)
}

var defaultTransformationMap = map[memo.Operand][]Transformation{
	memo.OperandSelection: {
		&PushSelDownTableScan{},
		&PushSelDownTableGather{},
	},
	memo.OperandDataSource: {
		&EnumeratePaths{},
	},
}

var patternMap = make(map[Transformation]*memo.Pattern)

// PushSelDownTableScan pushes the selection down to TableScan.
type PushSelDownTableScan struct {
}

// GetPattern implements Transformation interface. The pattern of this rule is `Selection -> TableScan`.
func (r *PushSelDownTableScan) GetPattern() *memo.Pattern {
	if p, ok := patternMap[r]; ok {
		return p
	}
	ts := memo.NewPattern(memo.OperandTableScan)
	p := memo.BuildPattern(memo.OperandSelection, ts)
	patternMap[r] = p
	return p
}

// Match implements Transformation interface.
func (r *PushSelDownTableScan) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
//
// It transforms `sel -> ts` to one of the following new exprs:
// 1. `newSel -> newTS`
// 2. `newTS`
//
// Filters of the old `sel` operator are removed if they are used to calculate
// the key ranges of the `ts` operator.
func (r *PushSelDownTableScan) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*plannercore.LogicalSelection)
	ts := old.Children[0].GetExpr().ExprNode.(*plannercore.TableScan)
	if ts.Handle == nil {
		return nil, false, false, nil
	}
	accesses, remained := ranger.DetachCondsForColumn(ts.SCtx(), sel.Conditions, ts.Handle)
	if accesses == nil {
		return nil, false, false, nil
	}
	newTblScan := plannercore.TableScan{
		Source:      ts.Source,
		Handle:      ts.Handle,
		AccessConds: ts.AccessConds.Shallow(),
	}.Init(ts.SCtx(), ts.SelectBlockOffset())
	newTblScan.AccessConds = append(newTblScan.AccessConds, accesses...)
	tblScanExpr := memo.NewGroupExpr(newTblScan)
	if len(remained) == 0 {
		// `sel -> ts` is transformed to `newTS`.
		return []*memo.GroupExpr{tblScanExpr}, true, false, nil
	}
	schema := old.GetExpr().Group.Prop.Schema
	tblScanGroup := memo.NewGroupWithSchema(tblScanExpr, schema)
	newSel := plannercore.LogicalSelection{Conditions: remained}.Init(sel.SCtx(), sel.SelectBlockOffset())
	selExpr := memo.NewGroupExpr(newSel)
	selExpr.Children = append(selExpr.Children, tblScanGroup)
	// `sel -> ts` is transformed to `newSel ->newTS`.
	return []*memo.GroupExpr{selExpr}, true, false, nil
}

// PushSelDownTableGather pushes the selection down to child of TableGather.
type PushSelDownTableGather struct {
}

// GetPattern implements Transformation interface. The pattern of this rule
// is `Selection -> TableGather -> Any`
func (r *PushSelDownTableGather) GetPattern() *memo.Pattern {
	if p, ok := patternMap[r]; ok {
		return p
	}
	any := memo.NewPattern(memo.OperandAny)
	tg := memo.BuildPattern(memo.OperandTableGather, any)
	p := memo.BuildPattern(memo.OperandSelection, tg)
	patternMap[r] = p
	return p
}

// Match implements Transformation interface.
func (r *PushSelDownTableGather) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
//
// It transforms `oldSel -> oldTg -> any` to one of the following new exprs:
// 1. `newTg -> pushedSel -> any`
// 2. `remainedSel -> newTg -> pushedSel -> any`
func (r *PushSelDownTableGather) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*plannercore.LogicalSelection)
	tg := old.Children[0].GetExpr().ExprNode.(*plannercore.TableGather)
	childGroup := old.Children[0].Children[0].Group
	var pushed, remained []expression.Expression
	sctx := tg.SCtx()
	_, pushed, remained = expression.ExpressionsToPB(sctx.GetSessionVars().StmtCtx, sel.Conditions, sctx.GetClient())
	if len(pushed) == 0 {
		return nil, false, false, nil
	}
	pushedSel := plannercore.LogicalSelection{Conditions: pushed}.Init(sctx, sel.SelectBlockOffset())
	pushedSelExpr := memo.NewGroupExpr(pushedSel)
	pushedSelExpr.Children = append(pushedSelExpr.Children, childGroup)
	pushedSelGroup := memo.NewGroupWithSchema(pushedSelExpr, childGroup.Prop.Schema)
	// The field content of TableGather would not be modified currently, so we
	// just reference the same tg instead of making a copy of it.
	//
	// TODO: if we save pushed filters later in TableGather, in order to do partition
	//       pruning or skyline pruning, we need to make a copy of the TableGather here.
	tblGatherExpr := memo.NewGroupExpr(tg)
	tblGatherExpr.Children = append(tblGatherExpr.Children, pushedSelGroup)
	if len(remained) == 0 {
		// `oldSel -> oldTg -> any` is transformed to `newTg -> pushedSel -> any`.
		return []*memo.GroupExpr{tblGatherExpr}, true, false, nil
	}
	tblGatherGroup := memo.NewGroupWithSchema(tblGatherExpr, pushedSelGroup.Prop.Schema)
	remainedSel := plannercore.LogicalSelection{Conditions: remained}.Init(sel.SCtx(), sel.SelectBlockOffset())
	remainedSelExpr := memo.NewGroupExpr(remainedSel)
	remainedSelExpr.Children = append(remainedSelExpr.Children, tblGatherGroup)
	// `oldSel -> oldTg -> any` is transformed to `remainedSel -> newTg -> pushedSel -> any`.
	return []*memo.GroupExpr{remainedSelExpr}, true, false, nil
}

// EnumeratePaths converts DataSource to table scan and index scans.
type EnumeratePaths struct {
}

// GetPattern implements Transformation interface. The pattern of this rule is `DataSource`.
func (r *EnumeratePaths) GetPattern() *memo.Pattern {
	if p, ok := patternMap[r]; ok {
		return p
	}
	p := memo.NewPattern(memo.OperandDataSource)
	patternMap[r] = p
	return p
}

// Match implements Transformation interface.
func (r *EnumeratePaths) Match(expr *memo.ExprIter) bool {
	return true
}

// OnTransform implements Transformation interface.
func (r *EnumeratePaths) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	ds := old.GetExpr().ExprNode.(*plannercore.DataSource)
	gathers := ds.Convert2Gathers()
	for _, gather := range gathers {
		expr := convert2GroupExpr(gather)
		newExprs = append(newExprs, expr)
	}
	return newExprs, true, false, nil
}
