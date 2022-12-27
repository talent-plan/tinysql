# System R Optimizer Framework

## Overview

Let's go back to this picture again.

![SQL](./imgs/proj4-1.png)

In project 2, we introduced the SQL Parser, which converts SQL text into an AST format that is easier for a database to process. Immediately after the Parser, we can see two processes: Validator and Type Infer. These correspond to SQL validity validation and the type system. In production environment, these two parts are very important. We first need to validate the legality of the SQL and be able to successfully recognize and convert within a pre-defined type system in order to facilitate further processing. However, these two parts are not included in TinySQL course contents. 

Next, we see two processes mentioned in the second line: Logical Optimizer and Physical Optimizer. These actually correspond to the concepts in the System R optimizer framework. Let's now delve into the content of this section.

## Optimizer based on the System R framework in TinySQL

The two-stage model of System R can be considered the pioneer of modern database cost-based optimizers. We will introduce the two-stage model in this section.

As seen in the above diagram, before entering the optimizer, the data remains in AST format, and then the optimizer generates the physical plan, which is the physical execution plan to be executed by the executor. In fact, before entering the optimizer, we have already converted the AST format data into the plan format. In `/planner/optimize.go:func Optimize()`, we can see:

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

As you can see, it actually shows that there are two optimizer framework in TinySQL, Cascades and System R, as we mentioned. The design concept of the Cascades architecture is explained in detail in [paper](https://15721.courses.cs.cmu.edu/spring2018/papers/15-optimizer1/graefe-ieee1995.pdf).

However, Cascades is not currently the optimizer framework that TinySQL focuses on. We will simply skip it here.

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

The logic of this function is very simple, it just calls `logicalOptimize`, `physicalOptimize`, and `postOptimize` in sequence. The first two are the optimization processes mentioned in the initial diagram, and the final `postOptimize` actually adjusts the execution plan after some optimizations, such as removing unnecessary projection operators.

### Logic optimization

Logical optimization has an entry at `planner/core/optimizer.go`. Logical optimization does not actually do anything complicated:
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

In logical optimization, we will iterate through the list of optimization rules and try to optimize the current execution plan tree. In TinySQL, we have
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
These rules will be explained using `columnPruner` as an example.

The code for each optimization rule is placed in a separate file under `planner/core`. Specifically, the implementation for `columnPruner` is located in `planner/core/rule_column_pruning.go`.

The purpose of column pruning is to eliminate unnecessary columns to save IO resources. For example, for a table t with columns a, b, c, and d:

```sql
select a from t where b > 5;
```

This query actually only needs to use columns a and b, so columns c and d are not needed and can be cut off.

The column cutting algorithm is implemented by going through the operators from top to bottom. The columns needed by a node are equal to the columns needed by itself plus the columns needed by its parent node. It can be observed that nodes from top to bottom will need more and more columns.

Column cutting mainly affects `Projection`, `DataSource`, and `Aggregation` operators because they are directly related to columns. `Projection` will cut off unused columns, and `DataSource` will also cut off unnecessary columns.

The `Aggregation` operator will involve the columns used in `group by` and the columns referenced in the aggregate functions. For example, in the query `select avg(a), sum(b) from t group by c d`, the c and d columns used in group by and the a and b columns used in the aggregate function are involved. Therefore, this `Aggregation` uses columns a, b, c, and d.

When `Selection` does column cutting, it needs to see which columns its parent node needs and which columns it needs to use in its own condition. `Sort` operation looks at which columns are used in `order by`. `Join` needs to take into account all the columns used in the join condition. In the specific code, each operator implements the `PruneColumns` interface:

```go
func (p *LogicalPlan) PruneColumns(parentUsedCols []*expression.Column)
```

After the column trimming operation, the various operators in the query plan will only record the columns that are actually needed.

Returning to the code, we can see that `rule.optimize` is the entry point for each rule in the logical optimization entrance:

```go
func (s *columnPruner) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	err := lp.PruneColumns(lp.Schema().Columns)
	return lp, err
}
```

Among them, `lp` is the root node of the planning tree and traverses from the root node, which means that the algorithm described above is carried out from the top down

### Physical optimization

The entry point of physical optimization is also in `/planner/core/optimizer.go`.

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


## Tasks

Complete the `TODO` content in `rule_predicate_push_down.go`.

## Rating

Passed the `TestPredicatePushDown` test under package `core`.

You can execute tests in the root directory using the `make test-proj4-1` command.

## Additional Content

If you're interested in the Cascades optimizer, you can try completing the TODO content in `transformation_rules.go`, and the corresponding tests are in `transformation_rules_test.go`.