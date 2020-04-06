# 事务

## 概览

在这个 Project 中，我们会介绍 TinySQL 中的事务模块。

## 理论简介

TinySQL 中的事务模块是基于 [Percolato](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Peng.pdf) 模型实现的 2PC 模型。在 [TiDB 最佳实践系列（三）乐观锁事务](https://pingcap.com/blog-cn/best-practice-optimistic-transaction/) 中对乐观锁的处理流程做了比较细致的讲解。

## 代码理解

### 2PC 简介

2PC 是实现分布式事务的一种方式，保证跨越多个网络节点的事务的原子性，不会出现事务只提交一半的问题。

在 TiDB，使用的 2PC 模型是 Google percolator 模型，简单的理解，percolator 模型和传统的 2PC 的区别主要在于消除了事务管理器的单点，把事务状态信息保存在每个 key 上，大幅提高了分布式事务的线性 scale 能力，虽然仍然存在一个 timestamp oracle 的单点，但是因为逻辑非常简单，而且可以 batch 执行，所以并不会成为系统的瓶颈。

关于 percolator 模型的细节，可以参考这篇文章的介绍 [https://pingcap.com/blog-cn/percolator-and-txn/](https://pingcap.com/blog-cn/percolator-and-txn/)

### 构造 twoPhaseCommitter

当一个事务准备提交的时候，会创建一个 [twoPhaseCommiter](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L67)，用来执行分布式的事务。

构造的时候，需要做以下几件事情

* [从 `memBuffer` 和 `lockedKeys` 里收集所有的 key 和 mutation](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L125)

    `memBuffer` 里的 key 是有序排列的，我们从头遍历 `memBuffer` 可以顺序的收集到事务里需要修改的 key，value 长度为 0 的 entry 表示 `DELETE` 操作，value 长度大于 0 表示 `PUT` 操作，`memBuffer` 里的第一个 key 做为事务的 primary key。`lockKeys` 里保存的是不需要修改，但需要加读锁的 key，也会做为 mutation 的 `LOCK ` 操作，写到 TiKV 上。

* [计算事务的大小是否超过限制](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L178)

    在收集 mutation 的时候，会统计整个事务的大小，如果超过了最大事务限制，会返回报错。

    太大的事务可能会让 TiKV 集群压力过大，执行失败并导致集群不可用，所以要对事务的大小做出硬性的限制。

* [计算事务的 TTL 时间](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L220)

    如果一个事务的 key 通过 `prewrite` 加锁后，事务没有执行完，tidb-server 就挂掉了，这时候集群内其他 tidb-server 是无法读取这个 key 的，如果没有 TTL，就会死锁。设置了 TTL 之后，读请求就可以在 TTL 超时之后执行清锁，然后读取到数据。

    我们计算一个事务的超时时间需要考虑正常执行一个事务需要花费的时间，如果太短会出现大的事务无法正常执行完的问题，如果太长，会有异常退出导致某个 key 长时间无法访问的问题。所以使用了这样一个算法，TTL 和事务的大小的平方根成正比，并控制在一个最小值和一个最大值之间。

### execute

在 twoPhaseCommiter 创建好以后，下一步就是执行 [execute](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L545) 函数。

在 `execute` 函数里，需要在 `defer` 函数里执行 [cleanupKeys](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L556)，在事务没有成功执行的时候，清理掉多余的锁，如果不做这一步操作，残留的锁会让读请求阻塞，直到 TTL 过期才会被清理。第一步会执行 [prewriteKeys](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L572)，如果成功，会从 PD 获取一个 `commitTS` 用来执行 `commit` 操作。取到了 `commitTS` 之后，还需要做以下验证:

* `commitTS` 比 `startTS` 大

* schema 没有过期

* 事务的执行时间没有过长

* 如果没有通过检查，事务会失败报错。

通过检查之后，执行最后一步 [commitKeys](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L607)，如果没有错误，事务就提交完成了。

当 `commitKeys` 请求遇到了网络超时，那么这个事务是否已经提交是不确定的，这时候不能执行 `cleanupKeys` 操作，否则就破坏了事务的一致性。我们对这种情况返回一个特殊的 [undetermined error](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L609)，让上层来处理。上层会在遇到这种 error 的时候，把连接断开，而不是返回给用户一个执行失败的错误。

[prewriteKeys](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L178532),  [commitKeys](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L537) 和 [cleanupKeys](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L541) 有很多相同的逻辑，需要把 keys 根据 region 分成 batch，然后对每个 batch 执行一次 RPC。

当 RPC 返回 region 过期的错误时，我们需要把这个 region 上的 keys 重新分成 batch，发送 RPC 请求。

这部分逻辑我们把它抽出来，放在 [doActionOnKeys](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L247) 和 [doActionOnBatches](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L307) 里，并实现 [actionPrewrite.handleSingleBatch](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L364)，[actionCommit.handleSingleBatch](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L429)，[actionCleanup.handleSingleBatch](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L501) 函数，用来执行单个 batch 的 RPC 请求。

虽然大部分逻辑是相同的，但是不同的请求在执行顺序上有一些不同，在 `doActionOnKeys` 里需要特殊的判断和处理。

* `prewrite` 分成的多个 batch 需要同步并行的执行。

* `commit` 分成的多个 batch 需要先执行第一个 batch，成功后再异步并行执行其他的 batch。

* `cleanup` 分成的多个 batch 需要异步并行执行。

[doActionOnBatches](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L307) 会开启多个 goroutines 并行的执行多个 batch，如果遇到了 error，会把其他正在执行的 `context cancel` 掉，然后返回第一个遇到的 error。

执行 `prewriteSingleBatch` 的时候，有可能会遇到 region 分裂错误，这时候 batch 里的 key 就不再是一个 region 上的 key 了，我们会在这里递归的调用 [prewriteKeys](https://github.com/pingcap-incubator/tinysql/blob/master/store/tikv/2pc.go#L380)，重新走一遍拆分 batch 然后执行 `doActionOnBatch` 和 `actionPrewrite.handleSingleBatch` 的流程。这部分逻辑在 `actionCommit.handleSingleBatc` 和 `actionCleanup.handleSingleBatch` 里也都有。

twoPhaseCommitter 包含的逻辑只是事务模型的一小部分，主要的逻辑在 tinykv 侧。详细的可以参考 tinykv lab4。