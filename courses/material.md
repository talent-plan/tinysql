#课程材料

##Overview

###关系代数

####SQL Grammar & Relation Algebra

#####课程资料

- https://cs186berkeley.net/static/notes/n0-SQLPart1.pdf
- https://cs186berkeley.net/static/notes/n1-SQLPart2.pdf
- https://15445.courses.cs.cmu.edu/fall2019/notes/01-introduction.pdf
- https://15445.courses.cs.cmu.edu/fall2019/notes/02-advancedsql.pdf

####Table Codec

#####课程资料
- KV mapping: [关系模型到 Key-Value 模型的映射](https://pingcap.com/blog-cn/tidb-internal-2/#%E5%85%B3%E7%B3%BB%E6%A8%A1%E5%9E%8B%E5%88%B0-key-value-%E6%A8%A1%E5%9E%8B%E7%9A%84%E6%98%A0%E5%B0%84)

##Parser

###课程资料

- Lexer & Parser
	- http://dinosaur.compilertools.net/
- `goyacc`
	- https://godoc.org/modernc.org/goyacc
	- https://pingcap.com/blog-cn/tidb-source-code-reading-5/

##Runtime

###Execution Model: Volcano, Vectorization and Compilation

####课程资料

- [Volcano - An Extensible and Parallel Query Evaluation System](https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf)
- [MonetDB/X100: Hyper-Pipelining Query Execution](http://cidrdb.org/cidr2005/papers/P19.pdf)
- [Efficiently Compiling Efficient Query Plans for Modern Hardware](http://www.vldb.org/pvldb/vol4/p539-neumann.pdf)
- [Relaxed Operator Fusion for In-Memory Databases: Making Compilation, Vectorization, and Prefetching Work Together At Last](http://www.vldb.org/pvldb/vol11/p1-menon.pdf)

###Join && Aggregate

####课程资料

- https://pingcap.com/blog-cn/tidb-source-code-reading-9/
- https://pingcap.com/blog-cn/tidb-source-code-reading-11/
- https://pingcap.com/blog-cn/tidb-source-code-reading-15/
- https://pingcap.com/blog-cn/tidb-source-code-reading-22/
- https://15721.courses.cs.cmu.edu/spring2019/papers/17-hashjoins/schuh-sigmod2016.pdf
- https://15721.courses.cs.cmu.edu/spring2019/papers/18-sortmergejoins/p85-balkesen.pdf

###Region Cache

####课程资料

- https://pingcap.com/blog-cn/tidb-source-code-reading-18/

###实现 region cache

####gRPC

######课程资料

- https://pingcap.com/blog-cn/tidb-source-code-reading-18/

##Optimizer

###Search Strategy: From System R to Cascades

####课程资料

- https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/chaudhuri-pods1998.pdf
- https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/graefe-ieee1995.pdf
- https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/p337-soliman.pdf
- https://pingcap.com/blog-cn/tidb-cascades-planner/

###Cost Model & Statistics

####课程资料

- https://15721.courses.cs.cmu.edu/spring2019/slides/24-costmodels.pdf
- https://15721.courses.cs.cmu.edu/spring2019/papers/24-costmodels/p204-leis.pdf
- https://pingcap.com/blog-cn/tidb-source-code-reading-12/

###Access Path Selection

####课程资料

- [Access Path Selection in a Relational Database Management System](https://people.eecs.berkeley.edu/~brewer/cs262/3-selinger79.pdf)

###Subquery Unnesting

####课程资料

- [Unnesting Arbitrary Queries](http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Neumann-Unnesting_Arbitrary_Querie.pdf)

###Join Reordering

####课程资料

- https://15721.courses.cs.cmu.edu/spring2017/papers/14-optimizer1/p539-moerkotte.pdf

##Transaction

###课程资料

- Isolation Levels
	- [Isolation (database systems)](https://en.wikipedia.org/wiki/Isolation_(database_systems))
	- [Transactions](https://cs186berkeley.net/static/notes/n10-Transactions.pdf)
- Concurrency Control
	- [15-445/645 Database Systems (Fall 2019) - Lecture Notes - 16 Concurrency Control Theory](https://15445.courses.cs.cmu.edu/fall2019/notes/16-concurrencycontrol.pdf)
	- [15-445/645 Database Systems (Fall 2019) - Lecture Notes - 19 Multi-Version Concurrency Control](https://15445.courses.cs.cmu.edu/fall2019/notes/19-multiversioning.pdf)

##Online, Asynchronous Schema Change

###课程资料

- [TiDB 源码阅读系列文章（十七）DDL 源码解析](https://pingcap.com/blog-cn/tidb-source-code-reading-17/)
- https://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/41376.pdf

