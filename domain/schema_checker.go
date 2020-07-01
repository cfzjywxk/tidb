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

package domain

import (
	"github.com/pingcap/tidb/metrics"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/store/tikv"
)

// SchemaChecker is used for checking schema-validity.
type SchemaChecker struct {
	SchemaValidator
	schemaVer       int64
	relatedTableIDs []int64
}

var (
	// SchemaOutOfDateRetryInterval is the backoff time before retrying.
	SchemaOutOfDateRetryInterval = int64(500 * time.Millisecond)
	// SchemaOutOfDateRetryTimes is the max retry count when the schema is out of date.
	SchemaOutOfDateRetryTimes = int32(10)
)

// NewSchemaChecker creates a new schema checker.
func NewSchemaChecker(do *Domain, schemaVer int64, relatedTableIDs []int64) *SchemaChecker {
	return &SchemaChecker{
		SchemaValidator: do.SchemaValidator,
		schemaVer:       schemaVer,
		relatedTableIDs: relatedTableIDs,
	}
}

// Check checks the validity of the schema version.
func (s *SchemaChecker) Check(txnTS uint64, getAllChangedInfo bool) (*tikv.RelatedSchemaChange, bool, error) {
	return s.CheckBySchemaVer(txnTS, s.schemaVer, getAllChangedInfo)
}

func (s *SchemaChecker) CheckBySchemaVer(txnTS uint64, startSchemaVer int64, getAllChangedInfo bool) (*tikv.RelatedSchemaChange, bool, error) {
	schemaOutOfDateRetryInterval := atomic.LoadInt64(&SchemaOutOfDateRetryInterval)
	schemaOutOfDateRetryTimes := int(atomic.LoadInt32(&SchemaOutOfDateRetryTimes))
	for i := 0; i < schemaOutOfDateRetryTimes; i++ {
		relatedChange, CheckResult := s.SchemaValidator.Check(txnTS, startSchemaVer, s.relatedTableIDs, getAllChangedInfo)
		switch CheckResult {
		case ResultSucc:
			return nil, false, nil
		case ResultFail:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			amendable := getAllChangedInfo && relatedChange != nil && (len(relatedChange.PhyTblIDS) > 0)
			if amendable {
				return relatedChange, amendable, ErrInfoSchemaChanged
			}
			return nil, false, ErrInfoSchemaChanged
		case ResultUnknown:
			time.Sleep(time.Duration(schemaOutOfDateRetryInterval))
		}

	}
	metrics.SchemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
	return nil, false, ErrInfoSchemaExpired
}
