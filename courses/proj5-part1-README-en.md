# Execution Models: Volcano Model and Vectorization

## Overview

In this section, we will introduce the most core execution model of the execution layer.

## Introduction to Execution Models

Before introducing a specific model, let's first think about why we need a model. A model is a simplification of the core characteristics of a real thing and can be used to help people understand the problem. In the previous SQL section, we can see that SQL can express complex and varied semantics, but at the same time it also has certain characteristics:

- SQL is made up of different parts, each with a fixed semantics
- There is a certain relationship between parts, and each section is a further processing of the results of the previous section

### Volcano model

Let's take a look at the volcano model first. In the volcano model, it consists of different actuators, each corresponding to a part of SQL, such as filtering, aggregation, etc.; the actuator and the executor form a tree-like relationship, and each operator implements three interfaces:

- 'Open', initialize the resources required by the current executor
- 'Next', takes the necessary data from the child node (if present), calculates and returns a result
- 'Close' to release the resources required by the actuator

![Volcano Execution Model](imgs/proj5-part1-1.png)

As can be seen from here, the volcano model is in line with our idea of the model. Each actuator is responsible for specific semantics and can be flexibly combined through a tree structure. So what are its drawbacks? If a large amount of data is processed, then each line of the output of each operator corresponds to a `Next` call, and the overhead of calling function on the framework will be very large.

### vectorize

One intuitive idea for reducing function calls is that `Next` returns a batch of data each time, rather than just one row. To support operations that return multiple rows, TinySQL also uses `Chunk` to represent these rows, which is used to reduce memory allocation overhead, reduce memory usage, and implement memory usage statistics/control.

Once the results are returned in batches, it also opens the possibility of vectorizing the computation, but first, let's take a look at the expression and its calculation.

## Understanding the Code

### vectorized expressions

There are three relatively simple vectorized string type function in [builtin\ _string\ _vec.go](https://github.com/tidb-incubator/tinysql/blob/course/expression/builtin_string_vec.go), which can be read in conjunction with [Perform computations using vectorization](https://docs.google.com/document/d/1JKP9YS3wYsuXsYhDgVepJt5y72K6_WxhUVfOLyjpAjc/edit#heading=h.66r4twnr3b1c).

### Volcano model

Let's use Selection as an example to introduce the code.

A relatively simple actuator, `Selection`, is implemented in [Executor.go #L346](https://github.com/tidb-incubator/tinysql/blob/course/executor/executor.go#L346). Its function is to filter out unnecessary lines according to `filters` and return them to the father. As you can see, it also implements common `Open`, `Next`, and `Close` interfaces. You can understand what it does by reading UnbatchedNext.

## Assignments

- Implement the vectorized expression [veEvalint](https://github.com/tidb-incubator/tinysql/blob/course/expression/builtin_string_vec.go#L89) and change the return value of [vectorized](https://github.com/tidb-incubator/tinysql/blob/course/expression/builtin_string_vec.go#L84) to `true`
- [Next](https://github.com/tidb-incubator/tinysql/blob/course/executor/executor.go#L380) function that implement vectorized selection.

## Tests

- Passed all tests under `expression`
- Through `TestSelectExec` in `executor/executor_test.go`.

You can run tests directly through `make test-proj5-1`, or run a specific function through `go test package_path -check.f func_name`. Taking `TestSelectExec` as an example, you can use `go test ./executor -check.f "TestSelectExec"` to run this specific function.

## Rating

`expression` and `executor` each account for 50%.
