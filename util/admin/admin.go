// Copyright 2015 PingCAP, Inc.
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

package admin

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"sort"
)

// DDLInfo is for DDL information.
type DDLInfo struct {
	SchemaVer   int64
	ReorgHandle int64        // It's only used for DDL information.
	Jobs        []*model.Job // It's the currently running jobs.
}

// GetDDLInfo returns DDL information.
func GetDDLInfo(txn kv.Transaction) (*DDLInfo, error) {
	var err error
	info := &DDLInfo{}
	t := meta.NewMeta(txn)

	info.Jobs = make([]*model.Job, 0, 2)
	job, err := t.GetDDLJobByIdx(0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if job != nil {
		info.Jobs = append(info.Jobs, job)
	}
	addIdxJob, err := t.GetDDLJobByIdx(0, meta.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if addIdxJob != nil {
		info.Jobs = append(info.Jobs, addIdxJob)
	}

	info.SchemaVer, err = t.GetSchemaVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if addIdxJob == nil {
		return info, nil
	}

	info.ReorgHandle, _, _, err = t.GetDDLReorgHandle(addIdxJob)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return info, nil
}

// IsJobRollbackable checks whether the job can be rollback.
func IsJobRollbackable(job *model.Job) bool {
	switch job.Type {
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		// We can't cancel if index current state is in StateDeleteOnly or StateDeleteReorganization, otherwise will cause inconsistent between record and index.
		if job.SchemaState == model.StateDeleteOnly ||
			job.SchemaState == model.StateDeleteReorganization {
			return false
		}
	case model.ActionDropSchema, model.ActionDropTable:
		// To simplify the rollback logic, cannot be canceled in the following states.
		if job.SchemaState == model.StateWriteOnly ||
			job.SchemaState == model.StateDeleteOnly {
			return false
		}
	case model.ActionDropColumn, model.ActionModifyColumn,
		model.ActionDropTablePartition, model.ActionAddTablePartition,
		model.ActionRebaseAutoID, model.ActionShardRowID,
		model.ActionModifyTableCharsetAndCollate,
		model.ActionModifySchemaCharsetAndCollate:
		return job.SchemaState == model.StateNone
	}
	return true
}

// CancelJobs cancels the DDL jobs.
func CancelJobs(txn kv.Transaction, ids []int64) ([]error, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	errs := make([]error, len(ids))
	t := meta.NewMeta(txn)
	generalJobs, err := getDDLJobsInQueue(t, meta.DefaultJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	addIdxJobs, err := getDDLJobsInQueue(t, meta.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := append(generalJobs, addIdxJobs...)

	for i, id := range ids {
		found := false
		for j, job := range jobs {
			if id != job.ID {
				logutil.BgLogger().Debug("the job that needs to be canceled isn't equal to current job",
					zap.Int64("need to canceled job ID", id),
					zap.Int64("current job ID", job.ID))
				continue
			}
			found = true
			// These states can't be cancelled.
			if job.IsDone() || job.IsSynced() {
				errs[i] = ErrCancelFinishedDDLJob.GenWithStackByArgs(id)
				continue
			}
			// If the state is rolling back, it means the work is cleaning the data after cancelling the job.
			if job.IsCancelled() || job.IsRollingback() || job.IsRollbackDone() {
				continue
			}
			if !IsJobRollbackable(job) {
				errs[i] = ErrCannotCancelDDLJob.GenWithStackByArgs(job.ID)
				continue
			}

			job.State = model.JobStateCancelling
			// Make sure RawArgs isn't overwritten.
			err := job.DecodeArgs(job.RawArgs)
			if err != nil {
				errs[i] = errors.Trace(err)
				continue
			}
			if job.Type == model.ActionAddIndex || job.Type == model.ActionAddPrimaryKey {
				offset := int64(j - len(generalJobs))
				err = t.UpdateDDLJob(offset, job, true, meta.AddIndexJobListKey)
			} else {
				err = t.UpdateDDLJob(int64(j), job, true)
			}
			if err != nil {
				errs[i] = errors.Trace(err)
			}
		}
		if !found {
			errs[i] = ErrDDLJobNotFound.GenWithStackByArgs(id)
		}
	}
	return errs, nil
}

func getDDLJobsInQueue(t *meta.Meta, jobListKey meta.JobListKeyType) ([]*model.Job, error) {
	cnt, err := t.DDLJobQueueLen(jobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*model.Job, cnt)
	for i := range jobs {
		jobs[i], err = t.GetDDLJobByIdx(int64(i), jobListKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return jobs, nil
}

// GetDDLJobs get all DDL jobs and sorts jobs by job.ID.
func GetDDLJobs(txn kv.Transaction) ([]*model.Job, error) {
	t := meta.NewMeta(txn)
	generalJobs, err := getDDLJobsInQueue(t, meta.DefaultJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	addIdxJobs, err := getDDLJobsInQueue(t, meta.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := append(generalJobs, addIdxJobs...)
	sort.Sort(jobArray(jobs))
	return jobs, nil
}

type jobArray []*model.Job

func (v jobArray) Len() int {
	return len(v)
}

func (v jobArray) Less(i, j int) bool {
	return v[i].ID < v[j].ID
}

func (v jobArray) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

// MaxHistoryJobs is exported for testing.
const MaxHistoryJobs = 10

// DefNumHistoryJobs is default value of the default number of history job
const DefNumHistoryJobs = 10

// GetHistoryDDLJobs returns the DDL history jobs and an error.
// The maximum count of history jobs is num.
func GetHistoryDDLJobs(txn kv.Transaction, maxNumJobs int) ([]*model.Job, error) {
	t := meta.NewMeta(txn)
	jobs, err := t.GetLastNHistoryDDLJobs(maxNumJobs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	jobsLen := len(jobs)
	if jobsLen > maxNumJobs {
		start := jobsLen - maxNumJobs
		jobs = jobs[start:]
	}
	jobsLen = len(jobs)
	ret := make([]*model.Job, 0, jobsLen)
	for i := jobsLen - 1; i >= 0; i-- {
		ret = append(ret, jobs[i])
	}
	return ret, nil
}

// RecordData is the record data composed of a handle and values.
type RecordData struct {
	Handle int64
	Values []types.Datum
}

var (
	// ErrDataInConsistent indicate that meets inconsistent data.
	ErrDataInConsistent = terror.ClassAdmin.New(mysql.ErrDataInConsistent, mysql.MySQLErrName[mysql.ErrDataInConsistent])
	// ErrDDLJobNotFound indicates the job id was not found.
	ErrDDLJobNotFound = terror.ClassAdmin.New(mysql.ErrDDLJobNotFound, mysql.MySQLErrName[mysql.ErrDDLJobNotFound])
	// ErrCancelFinishedDDLJob returns when cancel a finished ddl job.
	ErrCancelFinishedDDLJob = terror.ClassAdmin.New(mysql.ErrCancelFinishedDDLJob, mysql.MySQLErrName[mysql.ErrCancelFinishedDDLJob])
	// ErrCannotCancelDDLJob returns when cancel a almost finished ddl job, because cancel in now may cause data inconsistency.
	ErrCannotCancelDDLJob = terror.ClassAdmin.New(mysql.ErrCannotCancelDDLJob, mysql.MySQLErrName[mysql.ErrCannotCancelDDLJob])
)

func init() {
	// Register terror to mysql error map.
	mySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrDataInConsistent:     mysql.ErrDataInConsistent,
		mysql.ErrDDLJobNotFound:       mysql.ErrDDLJobNotFound,
		mysql.ErrCancelFinishedDDLJob: mysql.ErrCancelFinishedDDLJob,
		mysql.ErrCannotCancelDDLJob:   mysql.ErrCannotCancelDDLJob,
	}
	terror.ErrClassToMySQLCodes[terror.ClassAdmin] = mySQLErrCodes
}
