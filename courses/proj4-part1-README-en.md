# System R Optimizer Framework

## Overview

Let's go back to this picture again.

![SQL](./imgs/proj4-1.png)

We introduced in project 2 that SQL Parser can convert SQL text into an AST format that is easier for databases to process. After Parser, we can see two processes: Validator and Type Infer. These two parts correspond to SQL legality verification and type system. In an actual database system used in a production environment, these two parts are very important. We first need to verify the legitimacy of SQL, and be able to successfully identify and convert it in a predefined type system to facilitate further processing. However, since the content in this section is slightly boring, it is not in the TinySQL course content, so those of you who are interested can understand it on your own.

Next, we see two processes appearing one after the other in the second line, Logical Optimizer and Physical Optimizer. This actually corresponds to the concept in the System R optimizer framework. Let's move on to this section.

## System R Framework-based Optimizer in TinySQL

The System R two-stage model is thought to be the originator of the modern database cost-based optimizer. Due to the special nature of the optimizer framework, we'll introduce this concept through the code.

As can be seen from the figure above, before entering the optimizer, the data is still in AST format. Next, the optimizer will generate a physical plan, that is, the physical execution plan, which is executed by the executor. In fact, before entering the optimizer, we already converted data from AST format to Plan format.
In `/planner/optimize.go:func Optimize()` we can see:

```go
...
// build logical plan
...
p, err := builder.Build(ctx, node)
```

Here, node (AST) generates the corresponding p (Plan).

Next, at the end of this function, we can see the entrance to the optimizer:

```go
	// Handle the logical plan statement, use cascades planner if enabled.
	if sctx.GetSessionVars().EnableCascadesPlanner {
		finalPlan, err := cascades.DefaultOptimizer.FindBestPlan(sctx, logic)
		return finalPlan, names, err
	}
	finalPlan, err := plannercore.DoOptimize(ctx, builder.GetOptFlag(), logic)
```

As you can see, it actually shows that there are two optimizer framework in TinySQL, Cascades and System R as we mentioned. The design concept of the Cascades architecture is explained in detail in [paper](https://15721.courses.cs.cmu.edu/spring2018/papers/15-optimizer1/graefe-ieee1995.pdf).

[TiDB Cascades Planner principle analysis](https://pingcap.com/blog-cn/tidb-cascades-planner/) This article explains in detail the implement of Cascades in TiDB. You can learn about the implementation of Cascades in TiDB through this blog post in combination with TinySQL code. TiDB and TinySQL have exactly the same interpretation of key concepts. Conceptual nouns can be found directly in TinySQL.

However, Cascades is not currently the optimizer framework that TinySQL focuses on. We will simply skip it here, so those of you who are interested can understand it by yourselves.

The last line in the code snippet above is the entry point `plannercore.DoOptimize` to the System R optimizer, located in `planner/core/optimizer.go`
```go
// DoOptimize optimizes a logical plan to a physical plan.
func DoOptimize(ctx context.Context, flag uint64, logic LogicalPlan) (PhysicalPlan, error) {
	logic, err := logicalOptimize(ctx, flag, logic)
	if err != nil {
		return nil, err
	}
	physical, err := physicalOptimize(logic)
	if err != nil {
		return nil, err
	}
	finalPlan := postOptimize(physical)
	return finalPlan, nil
}
```

The logic of this function is very simple; it just calls `logicalOptimize`, `physicalOptimize`, and `postOptimize` in sequence. Among them, the first two are the optimization processes mentioned in the figure at the beginning, and the last `postOptimize` actually made some adjustments to the execution plan after some optimizations, such as removing the extra projection operator.

### Logic optimization

Logical optimization, or logical optimize, has an entry at `planner/core/optimizer.go`. What the logical optimization does is actually not complicated:
```go
func logicalOptimize(ctx context.Context, flag uint64, logic LogicalPlan) (LogicalPlan, error) {
	var err error
	for i, rule := range optRuleList {
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 {
			continue
		}
		logic, err = rule.optimize(ctx, logic)
		if err != nil {
			return nil, err
		}
	}
	return logic, err
}
```

In logical optimization, we go through the list of optimization rules in sequence and try to optimize the current execution plan tree. In TinySQL we have
```go
var optRuleList = []logicalOptRule{
	&columnPruner{},
	&buildKeySolver{},
	&aggregationEliminator{},
	&projectionEliminator{},
	&maxMinEliminator{},
	&ppdSolver{},
	&outerJoinEliminator{},
	&aggregationPushDownSolver{},
	&pushDownTopNOptimizer{},
	&joinReOrderSolver{},
}
```
These rules, we'll explain `columnPruner` as an example of column pruning.

The relevant code for each optimization rule is placed in a separate file under `planner/core`. Specifically, the specific implement of `columnPruner` is in `planner/core/rule_column_pruning.go`.

The purpose of column pruning is to trim columns that don't need to be read to save IO resources. For example, for table t with four columns a, b, c, d:
```sql
select a from t where b > 5;
```
This query actually only requires two columns a and b, so columns c and d don't need to be read, so you can crop them out.

The implement of the column pruning algorithm is to repeat the operator from top to bottom. The columns that a node needs are equal to the columns it needs to use, plus the columns that its parent node needs. As can be seen, nodes from top to bottom will require more and more columns to be used.

The operators that column pruning mainly affects are Projection, DataSource, and Aggregation, since they are directly related to columns. Unused columns will be cropped in Projection, and columns that are not needed will also be cropped out in DataSource.

What columns does the aggregation operator involve? The columns used for 'group by', and the columns referenced in the aggregation function. For example, `select avg(a), sum(b) from t group by c d`, here are the c and d columns used for group by, and the a and b columns used for the aggregation function. So this aggregation uses four columns a b c d.

When Selection does column pruning, it depends on which columns its parent node wants, and then which columns it uses in its own conditions. Sorting depends on which columns are used in 'order by'. Join takes into account all the columns used in the join conditions. In the specific code, each operator implement the pruneColumn interface:

```go
func (p *LogicalPlan) PruneColumns(parentUsedCols []*expression.Column)
```

After going through this step of column pruning, each operator in the query plan records only those columns it actually needs to use.

Back in the code, we can see at the logic optimization entrance that `rule.optimize` is the entrance to every rule, and the corresponding code is here
```go
func (s *columnPruner) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	err := lp.PruneColumns(lp.Schema().Columns)
	return lp, err
}
```

Among them, `lp` is the root node of the planning tree and traverses from the root node, that is, the algorithm described above is carried out from the top down.

### Physical optimization

Physical optimization, the entrance is also in `/planner/core/optimizer.go`.

The [PhysicalOptimize](https://github.com/pingcap-incubator/tinysql/blob/df75611ce926442bd6074b0f32b1351ca4aad925/planner/core/optimizer.go#L112) is the entrance to physical optimization. This process is actually a memorized search process.

The memorized search process can be roughly represented by the following pseudocode:

```
// The OrderProp tells whether the output data should be ordered by some column or expression. (e.g. For select * from t order by a, we need to make the data ordered by column a, that is the exactly information that OrderProp should store)
func findBestTask(p LogicalPlan, prop OrderProp) PhysicalPlan {

	if retP, ok := dpTable.Find(p, prop); ok {
		return retP
	}
	
	selfPhysicalPlans := p.getPhysicalPlans()
	
	bestPlanTree := a plan with maximum cost
	
	for _, pp := range selfPhysicalPlans {
	
		childProps := pp.GetChildProps(prop)
		childPlans := make([]PhysicalPlan, 0, len(p.children))
		for i, cp := range p.children {
			childPlans = append(childPlans, findBestTask(cp, childProps[i])
		}
		physicalPlanTree, cost := connect(pp, childPlans)
		
		if physicalPlanTree.cost < bestPlanTree.cost {
			bestPlanTree = physicalPlanTree
		}
	}
	return bestPlanTree
}
```

The actual execution code can be viewed in [findBestTask](https://github.com/pingcap-incubator/tinysql/blob/df75611ce926442bd6074b0f32b1351ca4aad925/planner/core/find_best_task.go#L95), and its logic is basically the same as the pseudocode described above.

For more details, please refer to [TiDB source code Read article (8) Cost-based optimization](https://pingcap.com/zh/blog/tidb-source-code-reading-8). The implement of TinySQL is completely consistent.

## Assignments

Complete the TODO content in `rule_predicate_push_down.go`.

## Rating

Passed the `TestPredicatePushDown` test under package `core`.

You can execute tests in the root directory using the `make test-proj4-1` command.

## Additional Content

If you're interested in the Cascades optimizer, we've also provided a less complete assignment. You can try completing the TODO content in `transformation_rules.go`, and the corresponding tests are in `transformation_rules_test.go`.

## References
- https://pingcap.com/zh/blog/tidb-source-code-reading-7
- https://pingcap.com/zh/blog/tidb-source-code-reading-8
