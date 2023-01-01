# Distributed Transactions: Percolator

## Overview

In this section, we will discuss the Percolator protocol.

## Percolator

[Percolator](https://research.google/pubs/pub36726/) is a distributed transaction processing protocol developed by Google and released in 2010. It involves dividing the submission of transactions into two stages: Prewrite and Commit, allowing for a distributed transaction database without the need for a coordinator. The protocol is used by Google's Spanner database with GPS and an atomic clock for global deployment, and is also employed by various other distributed databases. TiDB and TinySQL use this protocol as well.

## Introduction to the Execution Models

In TiDB, the execution process of the transaction is cached in a buffer, and, only when it is submitted, it is completely written to the distributed TiKV storage engine through the Percolator submission protocol. The entry point for this call is the `tikvTxn.Commit` function in `store/tikv/txn.go`.

During execution, transaction may be executed concurrently. We can use the Lock Resolve component to query the status of a transaction, and take actions accordingly.

## Two Phase Commit

The two-phase commit protocol of the Percolator is divided into Prewrite and Commit: Prewrite actually writes the data and Commit makes the data visible to others. In this process, the success of a transaction is atomically marked by the Primary Key. When Prewrite fails or Primary Key Commit fails, garbage collection is required to roll back the written transaction.

A Key within a transaction may involve different Regions. When performing a write operation on a Key, it needs to be sent to the correct Region to be processed. The `GroupKeysByRegion` function divides the Key into multiple batches by Region according to the region cache. However, it is possible that the corresponding storage node returns a Region Error due to an expired cache, in which case the batch needs to be split and retried.

### Task 1

Implement the `GroupKeysByRegion` function in `region_cache.go`.

During the execution process, there will be three types of operations involved, namely Prewrite/Commit/Rollback(Cleanup). These operations will be processed within the same process. You need to complete the `buildPrewriteRequest` function in the Prewrite process, and then follow the Prewrite's `handleSingleBatch` function to complete the `handleSingleBatch` function for Commit and Rollback.

## Lock Resolver

During the Prewrite stage, two records are written for a Key operation.

  - The Default CF stores the actual KV data.
  - The Lock CF stores the lock, including the Key and timestamp information, which will be cleared on successful Commit. The responsibility of the Lock Resolver is to handle the case of a transaction encountering a Lock during the commit process.

There may be several situations when a transaction encounters a Lock:

  - The transaction that owns the Lock has not yet committed this Key, and the Lock has not yet been cleared;
  - The transaction that owns the Lock has encountered an unrecoverable error and is in the process of rolling back, and has not yet cleared the Key;
  - The node of the transaction that owns the Lock has experienced an unexpected error, such as a node crash, and this node is no longer able to update it.

Under the Percolator protocol, the state of the transaction will be determined by querying the Primary Key of the Lock's owner, but when reading an uncompleted transaction (the Lock of the Primary Key has not yet been cleared), we expect to wait for the pending transaction to be completed and to clear any garbage data left by exceptions such as crash. At this point, ttl will be used to determine whether the transaction has expired, and if an expired transaction is encountered, it will be actively Rollbacked.

### Task 2

Implement the `getTxnStatus` and `resolveLock` functions in `lock_resolver.go`.

Besides during the process of committing a transaction, a transaction may also encounter a Lock when reading data, in which case the `ResolveLocks` function will be triggered. Implement the `tikvSnapshot.get` function in `snapshot.go`, so that the read request to run normally.

## Failpoint

[Failpoint](https://github.com/pingcap/failpoint) is a means of testing code using error injection. `Failpoint` in `Golang` is implement using code generation. In tests that use failpoint, you need to open failpoint first.

Open failpoint, the original code will be temporarily hidden to `{filename}__failpoint_stash__`, please do not delete it.

```sh
make failpoint-enable
```

Closing failpoint will recover the code in `{filename}__failpoint_stash__` to the original file and delete the stash file used for temporary storage.

```sh
make failpoint-disable
```

Since the `.go` file code using failpoint will be different when the failpoint switch state is different, please submit the code with failpoint turned off.

## Tests

Run `make proj6` and pass all test cases.

You can test the specified single use case or multiple use cases using the following command.

```sh
go test {package path} -check.f ^{regex}$

# example
go test -timeout 5s ./store/tikv -check.f ^TestFailAfterPrimary$
```
