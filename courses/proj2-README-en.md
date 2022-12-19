# Parser

## Introduction

In the Parser section, we'll cover how TinySQL transforms SQL into AST.

## The Process for SQL Processing

We have already known that, in the database, we use SQL statements to manipulate data. However, SQL itself is written in text. Database needs to validate the SQL statements before processing it. This is actually a very classic compilation problem in computer science. This step is similar to what a compiler does. Before we introduce the parser, we'll cover how SQL statements are processed in TinySQL.

![SQL](imgs/proj2-1.png)

The figure above shows how SQL statements are processed in TinySQL. In this part, we will talk about the parser.

## An introduction to Parser

The main function of the parser is to parse the text of an SQL statement based on predefined SQL syntax rules, and then transform it into an Abstract Syntax Tree (AST). An abstract syntax tree is a term in compilation in computer science. It represents the grammatical structure of a programming language in a tree form. For example, the SQL: ```select a from t where b > 0;``` would be converted to:

![AST](imgs/proj2-2.png)

We can read the AST from bottom up. It first reads data from table `t`, then it selects the data that satisfies the condition `t.b > 0`, and finally only column `t.a` is projected. The meaning of this AST is consistent with the meaning of the original SQL statement.

### An introduction to Lex & Yacc
In compilation, lexical analysis and grammatical analysis are used to generate AST from texts. `Lex and yacc` are tools used to generate lexical analyzers and parsers. In this project, we only need to focus on understanding the syntax definition file and how the generated parser works. Here's an example:

![Lex & Yacc](imgs/proj2-3.png)

The diagram shows the process of compiling using Lex and Yacc. Lex generates a lexical analyzer based on user-defined patterns. The lexical analyzer reads the source code and converts the source code into tokens output according to patterns. Yacc generates a syntax analyzer based on user-defined syntax rules. The syntax analyzer takes the tokens output from the lexical analyzer as input and creates a syntax tree based on the rules of the syntax. Finally, the syntax tree is traversed to generate output. The result can either generate machine code or interpret execution while iterating through the AST.

As shown in above process, users need to provide Lex patterns definitions and provide Yacc with syntax rules files separately.Lex & Yacc generate a lexical analyzer and syntax analyzer that meet their needs based on the input file provided by the user. Both of these configurations are text files and have the same structure:

```goyacc
... definitions ...
%%
... rules ...
%%
... subroutines ...
```

The content of the file is divided into three parts by `%%`, and we focus on the rule definition section in the middle. For the example above, Lex's input file is as follows:

```goyacc
...
%%
/* Variables */
[a-z]    {
            yylval = *yytext - 'a';
            return VARIABLE;
         }   
/* Integers */
[0-9]+   {
            yylval = atoi(yytext);
            return INTEGER;
         }
/* Operator */
[-+()=/*\n] { return *yytext; }
/* Skip spaces */
[ \t]    ;
/* Errors if others */
.        yyerror("invalid character");
%%
...
```

The above only lists the rule definition section. It can be seen that the rule uses regular expressions to define several kinds of tokens such as variables, integers, and operators. For example, an integer token is defined as follows:

```goyacc
[0-9]+  {
            yylval = atoi(yytext);
            return INTEGER; 
        }
```

When the input string matches this regular expression, the bracketed actions are executed: store the integer value in the variable yylval, and return the token type INTEGER to Yacc.

Let's take a look at the Yacc syntax rules definition file again:

```goyacc
%token INTEGER VARIABLE
%left '+' '-'
%left '*' '/'
...
%%

program:
        program statement '\n' 
        |
        ;

statement:
        expr                    { printf("%d\n", $1); }
        | VARIABLE '=' expr     { sym[$1] = $3; }
        ;
        
expr:
        INTEGER
        | VARIABLE              { $$ = sym[$1]; }
        | expr '+' expr         { $$ = $1 + $3; }
        | expr '-' expr         { $$ = $1 - $3; }
        | expr '*' expr         { $$ = $1 * $3; }
        | expr '/' expr         { $$ = $1 / $3; }
        | '(' expr ')'          { $$ = $2; }
        ;

%%
...
```

The first section defines the combination of token types and operators. All four types of operator are left associative. Operators on the same line have the same priority, and operator on different lines have higher priority for lines defined later.

The syntax grammatical rules use the BNF definition. BNF can be used to express context-free languages, and most modern programming languages can be expressed using BNF. The rules above define three types of generation. The item to the left of the colon in the generating formula (e.g., statement) is called a non-terminator, and INTEGER and VARIABLE are called terminators; they are tokens returned by Lex. The terminator can only appear on the right side of the generation formula. You can generate expressions using the generation-defined syntax:

```goyacc
expr -> expr * expr
     -> expr * INTEGER
     -> expr + expr * INTEGER
     -> expr + INTEGER * INTEGER
     -> INTEGER + INTEGER * INTEGER
```


Parsing an expression is the reverse operation of generating an expression. We need to reduce the expression to a non-terminator. The syntax analyzer generated by Yacc uses a bottom-up reduction (shift-reduce) method to parse the syntax while using the stack to save intermediate state. Let's take a look at the example. The parse process of the expression x+y*z:

```goyacc
1    . x + y * z
2    x . + y * z
3    expr . + y * z
4    expr + . y * z
5    expr + y . * z
6    expr + expr . * z
7    expr + expr * . z
8    expr + expr * z .
9    expr + expr * expr .
10   expr + expr .
11   expr .
12   statement .
13   program  .
```

The point (.) indicates the current reading position. With `.` moving from left to right, we push the token we read into the stack. When we find that the content in the stack matches the right side of a generation formula, the matching item is popped out of the stack, and the non-terminator on the left side of the generation formula is pushed into the stack. This process continues until all tokens have been read, and only the starting non-terminator (`program` in this case) remains on the stack.

Actions associated with this rule are defined in brackets on the right side of the generation formula, such as:

```goyacc
expr:  expr '*' expr         { $$ = $1 * $3; }
```

We replace the item in the stack that matches the right side of the generator with a non-terminator on the left side of the generator. In this example, we pop up expr '*' expr, and then push expr back to the stack. We can access items in the stack using $position, with $1 referring to the first item, $2 referring to the second item, and so on. $$ represents the top of the stack after the reduction operation has been performed. The action in this example is to pop three items out of the stack, add the two expressions, and push the result back to the top of the stack.

In the above example, the actions associated with the grammatical rules also complete the expression evaluation while completing the syntax parse. Generally, we want the result of the syntax parse to be an abstract syntax tree (AST), which can define the actions associated with the syntax rules like this:

```goyacc
...
%%
...
expr:
    INTEGER             { $$ = con($1); }
    | VARIABLE          { $$ = id($1); }
    | expr '+' expr     { $$ = opr('+', 2, $1, $3); }
    | expr '-' expr     { $$ = opr('-', 2, $1, $3); }
    | expr '*' expr     { $$ = opr('*', 2, $1, $3); } 
    | expr '/' expr     { $$ = opr('/', 2, $1, $3); }
    | '(' expr ')'      { $$ = $2; }
    ; 
%%
nodeType *con(int value) {
    ...
}
nodeType *id(int i) {
    ...
}
nodeType *opr(int oper, int nops, ...) {
    ...
}    
```

The above is a snippet of the syntax rule definition. We can see that the action associated with each rule is no longer a calculation, instead a corresponding function is called. The function returns the node type nodeType of the abstract syntax tree, and then pushes this node back to the stack. When the parse is complete, we get an abstract syntax tree composed of NodeType. An iterative visit to this syntax tree can be performed by the machine code or by the interpreter as well.

At this point, we have a general understanding of the principles of Lex & Yacc. There are actually a lot of details, such as how to unobscure the syntax. Since our goal is to use it in TinySQL, it is more than enough.

### An introduction to Goyacc

[Goyacc](https://github.com/cznic/goyacc) is the golang version of Yacc. Similar to the functionality of Yacc, goyacc generates a go language parser for that syntax rule based on the input syntax rules file. The parser YYParse generated by goyacc requires the lexical analyzer to conform to the following interface:

```go
type yyLexer interface {
    Lex(lval *yySymType) int
    Error(e string)
}
```

or

```go
type yyLexerEx interface {
    yyLexer
    // Hook for recording a reduction.
    Reduced(rule, state int, lval *yySymType) (stop bool) // Client should copy *lval.
}
```

TinySQL does not use tools like Lex to generate a lexical analyzer, but is entirely made in-house. The code corresponding to the lexical analyzer is parser/lexer.go, which implement the interface required by goyacc:

```go
...
// Scanner implements the yyLexer interface.
type Scanner struct {
    r   reader
    buf bytes.Buffer

    errs         []error
    stmtStartPos int

    // For scanning such kind of comment: /*! MySQL-specific code */ or /*+ optimizer hint */
    specialComment specialCommentScanner

    sqlMode mysql.SQLMode
}
// Lex returns a token and store the token value in v.
// Scanner satisfies yyLexer interface.
// 0 and invalid are special token id this function would return:
// return 0 tells parser that scanner meets EOF,
// return invalid tells parser that scanner meets illegal character.
func (s *Scanner) Lex(v *yySymType) int {
    tok, pos, lit := s.scan()
    v.offset = pos.Offset
    v.ident = lit
    ...
}
// Errors returns the errors during a scan.
func (s *Scanner) Errors() []error {
    return s.errs
}
```

In addition, Lexer uses trie technology for token identification. The specific implementation code is in `parser/misc.go`. 


### TinySQL Parser

At this point, we have the necessary prerequisite knowledge. Let's now look at our SQL syntax file `parser/parser.y`. goyacc will generate the corresponding SQL syntax parser based on this file.

`parser/parser.y` has quite a few lines, but it is still structured as described above:
```goyacc
... definitions ...
%%
... rules ...
%%
... subroutines ...
```

There is no content in the third part of `parser.y`. We only need to focus on the first part, definitions, and the second part,rules.

The first part mainly defines the type, priority, and integrability of tokens. Please pay attention to the union struct:

```goyacc
%union {
    offset int // offset
    item interface{}
    ident string
    expr ast.ExprNode
    statement ast.StmtNode
}
```

This union struct defines the properties and types of items that are pushed into the stack during syntactic parse.

The item pressed into the stack may be a terminator, or token, and its type may be item or ident.

This item may also be a non-terminator, that is, on the left side of the generated expression. Its type can be `expr`, `statement`, `item`, or `ident`.

Based on this union, goyacc generates the corresponding struct in the parse:

```go
type yySymType struct {
    yys       int
    offset    int // offset
    item      interface{}
    ident     string
    expr      ast.ExprNode
    statement ast.StmtNode
}
```

During syntax parsing, non-terminators are constructed as abstract syntax tree (AST) nodes `AST.EXPRNode` or `AST.stmtNode`. Data structures related to the abstract syntax tree are defined in the ast package, and most of them implement the ast.Node interface:

```go
// Node is the basic element of the AST.
// Interfaces embed Node should have 'Node' name suffix.
type Node interface {
    Accept(v Visitor) (node Node, ok bool)
    Text() string
    SetText(text string)
}
```

This interface has an Accept method, which accepts the Visitor parameter, and then processes the AST. It mainly relies on this Accept method to iterate through all nodes and perform structural transformation of the AST in Visitor mode.

```go
// Visitor visits a Node.
type Visitor interface {
    Enter(n Node) (node Node, skipChildren bool)
    Leave(n Node) (node Node, ok bool)
}
```

It is followed by a separate definition of tokens and non-terminators by type:

```goyacc
/*  ident type */
%token    <ident>
    ...
    add            "ADD"
    all             "ALL"
    alter            "ALTER"
    analyze            "ANALYZE"
    and            "AND"
    as            "AS"
    asc            "ASC"
    between            "BETWEEN"
    bigIntType        "BIGINT"
    ...

/*  item type */   
%token    <item>
    /*yy:token "1.%d"   */    floatLit        "floating-point literal"
    /*yy:token "1.%d"   */    decLit          "decimal literal"
    /*yy:token "%d"     */    intLit          "integer literal"
    /*yy:token "%x"     */    hexLit          "hexadecimal literal"
    /*yy:token "%b"     */    bitLit          "bit literal"

    andnot        "&^"
    assignmentEq    ":="
    eq        "="
    ge        ">="
    ...

/* non-terminator */
%type    <expr>
    Expression            "expression"
    BoolPri                "boolean primary expression"
    ExprOrDefault            "expression or default"
    PredicateExpr            "Predicate expression factor"
    SetExpr                "Set variable statement value's expression"
    ...

%type    <statement>
    AdminStmt            "Check table statement or show ddl statement"
    AlterTableStmt            "Alter table statement"
    AlterUserStmt            "Alter user statement"
    AnalyzeTableStmt        "Analyze table statement"
    BeginTransactionStmt        "BEGIN TRANSACTION statement"
    BinlogStmt            "Binlog base64 statement"
    ...
    
%type   <item>
    AlterTableOptionListOpt        "alter table option list opt"
    AlterTableSpec            "Alter table specification"
    AlterTableSpecList        "Alter table specification list"
    AnyOrAll            "Any or All for subquery"
    Assignment            "assignment"
    ...

%type    <ident>
    KeyOrIndex        "{KEY|INDEX}"
    ColumnKeywordOpt    "Column keyword or empty"
    PrimaryOpt        "Optional primary keyword"
    NowSym            "CURRENT_TIMESTAMP/LOCALTIME/LOCALTIMESTAMP"
    NowSymFunc        "CURRENT_TIMESTAMP/LOCALTIME/LOCALTIMESTAMP/NOW"
    ...
```

The first section includes a definition of priority and associativity:

```goyacc
...
%precedence sqlCache sqlNoCache
%precedence lowerThanIntervalKeyword
%precedence interval
%precedence lowerThanStringLitToken
%precedence stringLit
...
%right   assignmentEq
%left     pipes or pipesAsOr
%left     xor
%left     andand and
%left     between
...
```

The second part of the parser.y file is the SQL syntax generation formula and the corresponding action for each rule. The SQL syntax is very complicated, and most of the content of parser.y is a definition of a generator.

The SQL syntax can be found in the SQL Syntax section of the MySQL Reference Manual. For example, the SELECT syntax is defined as follows:

```sql
SELECT
    [ALL | DISTINCT | DISTINCTROW ]
      [HIGH_PRIORITY]
      [STRAIGHT_JOIN]
      [SQL_SMALL_RESULT] [SQL_BIG_RESULT] [SQL_BUFFER_RESULT]
      [SQL_CACHE | SQL_NO_CACHE] [SQL_CALC_FOUND_ROWS]
    select_expr [, select_expr ...]
    [FROM table_references
      [PARTITION partition_list]
    [WHERE where_condition]
    [GROUP BY {col_name | expr | position}
      [ASC | DESC], ... [WITH ROLLUP]]
    [HAVING where_condition]
    [ORDER BY {col_name | expr | position}
      [ASC | DESC], ...]
    [LIMIT {[offset,] row_count | row_count OFFSET offset}]
    [PROCEDURE procedure_name(argument_list)]
    [INTO OUTFILE 'file_name'
        [CHARACTER SET charset_name]
        export_options
      | INTO DUMPFILE 'file_name'
      | INTO var_name [, var_name]]
    [FOR UPDATE | LOCK IN SHARE MODE]]
```

We can find the generation formula for the SELECT statement in parser.y:

```goyacc
SelectStmt:
    "SELECT" SelectStmtOpts SelectStmtFieldList OrderByOptional SelectStmtLimit SelectLockOpt
    { ... }
|   "SELECT" SelectStmtOpts SelectStmtFieldList FromDual WhereClauseOptional SelectStmtLimit SelectLockOpt
    { ... }  
|   "SELECT" SelectStmtOpts SelectStmtFieldList "FROM"
    TableRefsClause WhereClauseOptional SelectStmtGroup HavingClause OrderByOptional
    SelectStmtLimit SelectLockOpt
    { ... } 
```

The generative selectStmt and SELECT syntaxes correspond.

Omitting actions in curly brackets, this part of the code constructs AST's ast.selectstmt (parser/ast/dml.go) node:

```go
type SelectStmt struct {
    dmlNode
    resultSetNode

    // SelectStmtOpts wraps around select hints and switches.
    *SelectStmtOpts
    // Distinct represents whether the select has distinct option.
    Distinct bool
    // From is the from clause of the query.
    From *TableRefsClause
    // Where is the where clause in select statement.
    Where ExprNode
    // Fields is the select expression list.
    Fields *FieldList
    // GroupBy is the group by expression list.
    GroupBy *GroupByClause
    // Having is the having condition.
    Having *HavingClause
    // OrderBy is the ordering expression list.
    OrderBy *OrderByClause
    // Limit is the limit clause.
    Limit *Limit
    // LockTp is the lock type
    LockTp SelectLockType
    // TableHints represents the level Optimizer Hint
    TableHints []*TableOptimizerHint
}
```

It is obvious that the contents contained in the `ast.selectStmt` structure corresponds to the `SELECT` syntax.

Other generation expressions are also written according to the corresponding SQL syntax. As you can see from `parser.y`'s comments, this file was initially generated from the BNF conversion tool. Writing this rules file by hand from scratch would be a huge amount of work.

Once you've defined the parser.y syntax rules file, you can use goyacc to generate a syntax parse:

```bash
cd parser
make
```

Note: You can check the Makefile for specific commands. Also, the command includes a format check for parser.y and automatic formatting. If a format-related error occurs, `make` needs to be executed again.

## Task

Implement `JoinTable`, you can use the failed tests in the parser test to determine what syntax parts need to be added.

## Tests

Pass the test `TestDMLStmt`.

Once the code is complete, execute it in the root directory

```bash
cd parser
make
```

Then the tinysql parser can be generated. This command includes checking the format of parser.y and automatically organizing the format. If an error related to the format occurs, you need to execute the `make` command again.

```bash
cd .. 
make test-proj2
```
Execute tests

## Grading

Pass the test `TestDMLStmt`.

## References

- https://pingcap.com/zh/blog/tidb-source-code-reading-5
- https://github.com/cznic/goyacc
- http://dinosaur.compilertools.net/
- https://dev.mysql.com/doc/refman/8.0/en/
