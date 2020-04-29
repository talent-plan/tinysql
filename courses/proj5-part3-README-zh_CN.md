# Hash Aggregate

## 概览

在这一小节，我们将深入讲解 Hash Agg 的原理和实现。

## Hash Agg 简介

在理解 Hash 聚合之前，我们先回顾下聚合。以 `select sum(b) from t group by a` 为例，它的意思就是求那些具有相同 a 列值的 b 列上的和，也就是说最终每个不同的 a 只会输出一行，那么其实最终的结果可以认为是一个类似 map<a, sum(b)> 这样的 map，那么而更新 map 的值也比较简单。

怎样进行优化呢？一个观察是 sum 这样的函数是满足交换律和结合律的，也就是说相加的顺序没有关系，例如假设某个具体的 a 值具有 10 行，那么我们可以先计算其中 5 行的 sum(b) 值，再计算另外 5 行 sum(b) 值，再把这样的结果合并起来。

对于 avg 这样的函数怎样优化呢？我们可以将它拆成是两个聚合函数，即 sum(b) 和 count(b)，两个函数都满足交换律和结合律，因此也可以用上面的方法去优化。

图

那么一个可行的优化就是我们引入一个中间状态，这个时候每个 partial worker  从孩子节点取一部分数据并预先计算，注意这个时候得到的结果可能是不正确的，然后再将结果交给一个合并 partial worker 结果的 final worker，这样就可以提高整体的执行效率。

但上面的模型可能的一个结果就是有可能 a 列上不同值的数目较多，导致 final worker 成为瓶颈。怎样优化这个瓶颈呢？一个想法是将 fianl woker 拆成多个，但这个时候怎样保证正确性呢？即怎样让具有相同 a 值的、由 partial worker 所计算的中间结果映射到同一个 final worker 上？这个时候哈希函数又可以排上用场了，我们可以将 a 上的值进行哈希，然后将具有相同哈希值的中间结果交给同一个 final worker，即：

图

这样我们就得到了 TinySQL 中 Hash Agg 的执行模型。
为了适应上述并行计算的，TiDB 对于聚合函数的计算阶段进行划分，相应定义了 4 种计算模式：CompleteMode，FinalMode，Partial1Mode，Partial2Mode。不同的计算模式下，所处理的输入值和输出值会有所差异，如下表所示：

| AggFunctionMode | 输入值 | 输出值 |
| :-------------- | ----: | ----: |
|CompleteMode|原始数据|最终结果|
|FinalMode|中间结果|最终结果|
|Partial1Mode|原始数据|中间结果|
|Partial2Mode|中间结果|进一步聚合的中间结果|

以上文提到的 `select avg(b) from t group by a` 为例，通过对计算阶段进行划分，可以有多种不同的计算模式的组合，如：

- CompleteMode

此时 AVG 函数 的整个计算过程只有一个阶段，如图所示：

图

- Partial1Mode --> FinalMode

此时我们将 AVG 函数的计算过程拆成两个阶段进行，如图所示：

图

除了上面的两个例子外，还可能聚合被下推到 TinyKV 上进行计算（Partial1Mode），并返回经过预聚合的中间结果。为了充分利用 TinySQL 所在机器的 CPU 和内存资源，加快 TinySQL 层的聚合计算，TinySQL 层的聚合函数计算可以这样进行：Partial2Mode --> FinalMode。

## 理解代码

TiDB 的并行 Hash Aggregation 算子执行过程中的主要线程有：Main Thead，Data Fetcher，Partial Worker，和 Final Worker：

- Main Thread 一个：
	- 启动 Input Reader，Partial Workers 及 Final Workers
	- 等待 Final Worker 的执行结果并返回
- Data Fetcher 一个：
	- 按 batch 读取子节点数据并分发给 Partial Worker
- Partial Worker 多 个：
	- 读取 Data Fetcher 发送来的数据，并做预聚合
	- 将预聚合结果根据 Group 值 shuffle 给对应的 Final Worker
- Final Worker 多 个：
	- 读取 PartialWorker 发送来的数据，计算最终结果，发送给 Main Thread

Hash Aggregation 的执行阶段可分为如下图所示的 5 步：

图

1. 启动 Data Fetcher，Partial Workers 及 Final Workers

	这部分工作由 prepare4ParallelExec 函数完成。该函数会启动一个 Data Fetcher，多个 Partial Worker 以及多个 Final Worker。
	
2. DataFetcher 读取子节点的数据并分发给 Partial Workers
	
	这部分工作由 fetchChildData 函数完成。
	
3. Partial Workers 预聚合计算，及根据 Group Key shuffle 给对应的 Final Workers

	这部分工作由 HashAggPartialWorker.run 函数完成。该函数调用 updatePartialResult 函数对 DataFetcher 发来数据执行预聚合计算，并将预聚合结果存储到 partialResultMap 中。其中 partialResultMap 的 key 为根据 Group-By 的值 encode 的结果，value 为 PartialResult 类型的数组，数组中的每个元素表示该下标处的聚合函数在对应 Group 中的预聚合结果。shuffleIntermData 函数完成根据 Group 值 shuffle 给对应的 Final Worker。

4. Final Worker 计算最终结果，发送给 Main Thread

	这部分工作由 HashAggFinalWorker.run 函数完成。该函数调用 consumeIntermData 函数接收 PartialWorkers 发送来的预聚合结果，进而合并得到最终结果。getFinalResult 函数完成发送最终结果给 Main Thread。

5. Main Thread 接收最终结果并返回

## 作业描述

补充完整 `aggregate.go` 中的 TODO 代码

## 评分

完成 `aggregate_test.go` 中的测试


