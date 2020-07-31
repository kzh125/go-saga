// Package saga provide a framework for Saga-pattern to solve distribute transaction problem.
// In saga-pattern, Saga is a long-lived transaction came up with many small sub-transaction.
// ExecutionCoordinator(SEC) is coordinator for sub-transactions execute and saga-log written.
// Sub-transaction is normal business operation, it contain a Action and action's Compensate.
// Saga-Log is used to record saga process, and SEC will use it to decide next step and how to recovery from error.
//
// There is a great speak for Saga-pattern at https://www.youtube.com/watch?v=xDuwrtwYHu8
package saga

import (
	"fmt"
	"reflect"
	"time"

	"context"

	"github.com/kzh125/go-saga/storage"
)

const LogPrefix = "saga"

// Saga presents current execute transaction.
// A Saga constituted by small sub-transactions.
type Saga struct {
	id      string
	logID   string
	context context.Context
	sec     *ExecutionCoordinator
	err     error
	abort   bool
	store   storage.Storage
}

func (s *Saga) startSaga() {
	log := &Log{
		Type: SagaStart,
		Time: time.Now(),
	}
	err := s.store.AppendLog(s.logID, log.mustMarshal())
	if err != nil {
		panic(fmt.Errorf("startSaga AppendLog: %v", err))
	}
}

// ExecSub executes a sub-transaction for given subTxID(which define in SEC initialize) and arguments.
// it returns current Saga.
func (s *Saga) ExecSub(subTxID string, args ...interface{}) *Saga {
	subTxDef := s.sec.MustFindSubTxDef(subTxID)
	log := &Log{
		Type:    ActionStart,
		SubTxID: subTxID,
		Time:    time.Now(),
		Params:  MarshalParam(s.sec, args),
	}
	err := s.store.AppendLog(s.logID, log.mustMarshal())
	if err != nil {
		panic(fmt.Errorf("ExecSub AppendLog: %v", err))
	}

	params := make([]reflect.Value, 0, len(args)+1)
	params = append(params, reflect.ValueOf(s.context))
	for _, arg := range args {
		params = append(params, reflect.ValueOf(arg))
	}
	result := subTxDef.action.Call(params)
	if isReturnError(result) {
		s.Abort()
		return s
	}

	log = &Log{
		Type:    ActionEnd,
		SubTxID: subTxID,
		Time:    time.Now(),
	}
	err = s.store.AppendLog(s.logID, log.mustMarshal())
	if err != nil {
		panic(fmt.Errorf("ExecSub AppendLog: %v", err))
	}
	return s
}

// EndSaga finishes a Saga's execution.
func (s *Saga) EndSaga() {
	log := &Log{
		Type: SagaEnd,
		Time: time.Now(),
	}
	err := s.store.AppendLog(s.logID, log.mustMarshal())
	if err != nil {
		panic(fmt.Errorf("EndSaga AppendLog: %v", err))
	}
	err = s.store.Cleanup(s.logID)
	if err != nil {
		panic(fmt.Errorf("EndSaga Cleanup: %v", err))
	}
}

// Abort stop and compensate to rollback to start situation.
// This method will stop continue sub-transaction and do Compensate for executed sub-transaction.
// SubTx will call this method internal.
func (s *Saga) Abort() {
	logs, err := s.store.Lookup(s.logID)
	if err != nil {
		panic(fmt.Errorf("Abort Lookup: %v", err))
	}
	alog := &Log{
		Type: SagaAbort,
		Time: time.Now(),
	}
	err = s.store.AppendLog(s.logID, alog.mustMarshal())
	if err != nil {
		panic(fmt.Errorf("Abort AppendLog: %v", err))
	}
	for i := len(logs) - 1; i >= 0; i-- {
		logData := logs[i]
		log := mustUnmarshalLog(logData)
		if log.Type == ActionStart {
			if err := s.compensate(log); err != nil {
				panic("Compensate Failure..")
			}
		}
	}
}

func (s *Saga) compensate(tlog Log) error {
	clog := &Log{
		Type:    CompensateStart,
		SubTxID: tlog.SubTxID,
		Time:    time.Now(),
	}
	err := s.store.AppendLog(s.logID, clog.mustMarshal())
	if err != nil {
		panic(fmt.Errorf("compensate AppendLog: %v", err))
	}

	args := UnmarshalParam(s.sec, tlog.Params)

	params := make([]reflect.Value, 0, len(args)+1)
	params = append(params, reflect.ValueOf(s.context))
	params = append(params, args...)

	subDef := s.sec.MustFindSubTxDef(tlog.SubTxID)
	result := subDef.compensate.Call(params)
	if isReturnError(result) {
		s.Abort()
	}

	clog = &Log{
		Type:    CompensateEnd,
		SubTxID: tlog.SubTxID,
		Time:    time.Now(),
	}
	err = s.store.AppendLog(s.logID, clog.mustMarshal())
	if err != nil {
		panic(fmt.Errorf("compensate AppendLog: %v", err))
	}
	return nil
}

func isReturnError(result []reflect.Value) bool {
	if len(result) == 1 && !result[0].IsNil() {
		return true
	}
	return false
}
