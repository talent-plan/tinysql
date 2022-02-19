%{

package parser

import (
	"strings"

	"tinysql/parser/mysql"
	"tinysql/parser/ast"
	"tinysql/parser/model"
	"tinysql/parser/opcode"
	"tinysql/parser/charset"
	"tinysql/parser/types"
)
%}

%union {
	offset int
	item interface{}
	ident string
	expr ast.ExprNode
	statement ast.StmtNode
}

%token	<ident>
	selectKwd		"SELECT"
	from			"FROM"
	as			    "AS"
	join			"JOIN"
	inner 			"INNER"
	where			"WHERE"
    xor 			"XOR"
    not			    "NOT"
    null			"NULL"
    div 			"DIV"
    mod 			"MOD"
    trueKwd			"TRUE"
    falseKwd		"FALSE"
    or			    "OR"
    and			    "AND"
    is			    "IS"
    in			    "IN"
	andand			"&&"
	pipes			"||"


%token	<item>

	/*yy:token "1.%d"   */	floatLit        "floating-point literal"
	/*yy:token "1.%d"   */	decLit          "decimal literal"
	/*yy:token "%d"     */	intLit          "integer literal"

	eq		"="
	ge		">="
	le		"<="
	lsh		"<<"
	neq		"!="
	neqSynonym	"<>"
	nulleq		"<=>"
	rsh		">>"

%token not2

%token	<ident>

    /*yy:token "%c"     */	identifier      "identifier"
    /*yy:token "\"%c\"" */	stringLit       "string literal"
    Identifier			"identifier or unreserved keyword"

%type	<statement>
	SelectStmt			"SELECT statement"

%type   <item>
    EscapedTableRef 	"escaped table reference"
	Field				"field expression"
	FieldList			"field expression list"
	SelectStmtFieldList	"SELECT statement field list"
	SelectStmtBasic		"SELECT statement from constant value"
	SelectStmtFromTable	"SELECT statement from table"
    TableAsName			"table alias name"
    TableAsNameOpt 		"table alias name optional"
    TableRefsClause		"Table references clause"
	TableRef 			"table reference"
	TableRefs 			"table references"
	TableFactor 		"table factor"
    TableName			"Table name"

    JoinTable 			"join table"
    WhereClauseOptional	"Optional WHERE clause"
    WhereClause		    "WHERE clause"
    IsOrNotOp		    "Is predicate"
    InOrNotOp		    "In predicate"
    
%type	<expr>
    Expression			"expression"
    BoolPri				"boolean primary expression"
    PredicateExpr		"Predicate expression factor"
    BitExpr				"bit expression"
    SimpleExpr			"simple expression"
    SimpleIdent			"Simple Identifier expression"
    Literal				"literal value"
    StringLiteral		"text literal"



%precedence lowerThanStringLitToken
%precedence stringLit
%precedence lowerThanSetKeyword

%left   join  inner
/* A dummy token to force the priority of TableRef production in a join. */
%left   tableRefPriority
%left 	pipes or pipesAsOr
%left 	xor
%left 	andand and
%left 	between
%left 	eq ge le neq neqSynonym '>' '<' is in
%left 	'|'
%left 	'&'
%left 	rsh lsh
%left 	'-' '+'
%left 	'*' '/' '%' div mod
%left 	'^'
%left 	'~' neg
%right 	not not2

%precedence '('
%precedence ','

%start	Start

%%

Start:
	StatementList

StatementList:
	Statement
	{
		if $1 != nil {
			s := $1
			if lexer, ok := yylex.(stmtTexter); ok {
				s.SetText(lexer.stmtText())
			}
			parser.result = append(parser.result, s)
		}
	}
|	StatementList ';' Statement
	{
		if $3 != nil {
			s := $3
			if lexer, ok := yylex.(stmtTexter); ok {
				s.SetText(lexer.stmtText())
			}
			parser.result = append(parser.result, s)
		}
	}

Statement:
    SelectStmt

SelectStmt:
    SelectStmtFromTable
	{
		st := $1.(*ast.SelectStmt)
		$$ = st
	}

SelectStmtFromTable:
	SelectStmtBasic "FROM"
	TableRefsClause WhereClauseOptional
	{
		st := $1.(*ast.SelectStmt)
		st.From = $3.(*ast.TableRefsClause)
		lastField := st.Fields.Fields[len(st.Fields.Fields)-1]
		if lastField.Expr != nil && lastField.AsName.O == "" {
			lastEnd := parser.endOffset(&yyS[yypt-4])
			lastField.SetText(parser.src[lastField.Offset:lastEnd])
		}
		if $4 != nil {
			st.Where = $4.(ast.ExprNode)
		}
		$$ = st
	}

SelectStmtBasic:
	"SELECT" SelectStmtFieldList
	{
		st := &ast.SelectStmt {
			Fields:        $2.(*ast.FieldList),
		}
		$$ = st
	}

SelectStmtFieldList:
	FieldList
	{
		$$ = &ast.FieldList{Fields: $1.([]*ast.SelectField)}
	}

FieldList:
	Field
	{
		field := $1.(*ast.SelectField)
		field.Offset = parser.startOffset(&yyS[yypt])
		$$ = []*ast.SelectField{field}
	}
|	FieldList ',' Field
	{

		fl := $1.([]*ast.SelectField)
		last := fl[len(fl)-1]
		if last.Expr != nil && last.AsName.O == "" {
			lastEnd := parser.endOffset(&yyS[yypt-1])
			last.SetText(parser.src[last.Offset:lastEnd])
		}
		newField := $3.(*ast.SelectField)
		newField.Offset = parser.startOffset(&yyS[yypt])
		$$ = append(fl, newField)
	}

Field:
	'*'
	{
		$$ = &ast.SelectField{WildCard: &ast.WildCardField{}}
	}
|	Identifier '.' '*'
	{
		wildCard := &ast.WildCardField{Table: model.NewCIStr($1)}
		$$ = &ast.SelectField{WildCard: wildCard}
	}
|	Identifier '.' Identifier '.' '*'
	{
		wildCard := &ast.WildCardField{Schema: model.NewCIStr($1), Table: model.NewCIStr($3)}
		$$ = &ast.SelectField{WildCard: wildCard}
	}

TableRefsClause:
	TableRefs
	{
		$$ = &ast.TableRefsClause{TableRefs: $1.(*ast.Join)}
	}

TableRefs:
	EscapedTableRef
	{
		if j, ok := $1.(*ast.Join); ok {
			// if $1 is Join, use it directly
			$$ = j
		} else {
			$$ = &ast.Join{Left: $1.(ast.ResultSetNode), Right: nil}
		}
	}
|	TableRefs ',' EscapedTableRef
	{
		/* from a, b is default cross join */
		$$ = &ast.Join{Left: $1.(ast.ResultSetNode), Right: $3.(ast.ResultSetNode), Tp: ast.CrossJoin}
	}

EscapedTableRef:
	TableRef %prec lowerThanSetKeyword
	{
		$$ = $1
	}
|	'{' Identifier TableRef '}'
	{
		/*
		* ODBC escape syntax for outer join is { OJ join_table }
		* Use an Identifier for OJ
		*/
		$$ = $3
	}

TableRef:
	TableFactor
	{
		$$ = $1
	}
|	JoinTable
	{
		$$ = $1
	}

TableFactor:
	TableName TableAsNameOpt
	{
		tn := $1.(*ast.TableName)
		tn.IndexHints = $3.([]*ast.IndexHint)
		$$ = &ast.TableSource{Source: tn, AsName: $2.(model.CIStr)}
	}

TableName:
	Identifier
	{
		$$ = &ast.TableName{Name:model.NewCIStr($1)}
	}
|	Identifier '.' Identifier
	{
		$$ = &ast.TableName{Schema:model.NewCIStr($1),	Name:model.NewCIStr($3)}
	}

TableAsNameOpt:
	{
		$$ = model.CIStr{}
	}
|	TableAsName
	{
		$$ = $1
	}

TableAsName:
	Identifier
	{
		$$ = model.NewCIStr($1)
	}
|	"AS" Identifier
	{
		$$ = model.NewCIStr($2)
	}

JoinTable:
	/* Use %prec to evaluate production TableRef before cross join */
	TableRef CrossOpt TableRef %prec tableRefPriority
	{
		$$ = &ast.Join{Left: $1.(ast.ResultSetNode), Right: $3.(ast.ResultSetNode), Tp: ast.CrossJoin}
	}

CrossOpt:
	"JOIN"
|	"INNER" "JOIN"

WhereClauseOptional:
	{
		$$ = nil
	}
|	WhereClause
	{
		$$ = $1
	}

WhereClause:
	"WHERE" Expression
	{
		$$ = $2
	}

Expression:
	Expression logOr Expression %prec pipes
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.LogicOr, L: $1, R: $3}
	}
|	Expression "XOR" Expression %prec xor
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.LogicXor, L: $1, R: $3}
	}
|	Expression logAnd Expression %prec andand
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: $1, R: $3}
	}
|	"NOT" Expression %prec not
	{
		$$ = &ast.UnaryOperationExpr{Op: opcode.Not, V: $2}
	}
|	BoolPri

BoolPri:
	BoolPri IsOrNotOp "NULL" %prec is
	{
		$$ = &ast.IsNullExpr{Expr: $1, Not: !$2.(bool)}
	}
|	BoolPri CompareOp PredicateExpr %prec eq
	{
		$$ = &ast.BinaryOperationExpr{Op: $2.(opcode.Op), L: $1, R: $3}
	}
|	PredicateExpr

PredicateExpr:
	BitExpr

BitExpr:
	BitExpr '|' BitExpr %prec '|'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Or, L: $1, R: $3}
	}
|	BitExpr '&' BitExpr %prec '&'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.And, L: $1, R: $3}
	}
|	BitExpr "<<" BitExpr %prec lsh
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.LeftShift, L: $1, R: $3}
	}
|	BitExpr ">>" BitExpr %prec rsh
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.RightShift, L: $1, R: $3}
	}
|	BitExpr '+' BitExpr %prec '+'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Plus, L: $1, R: $3}
	}
|	BitExpr '-' BitExpr %prec '-'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Minus, L: $1, R: $3}
	}
|	BitExpr '*' BitExpr %prec '*'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Mul, L: $1, R: $3}
	}
|	BitExpr '/' BitExpr %prec '/'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Div, L: $1, R: $3}
	}
|	BitExpr '%' BitExpr %prec '%'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Mod, L: $1, R: $3}
	}
|	BitExpr "DIV" BitExpr %prec div
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.IntDiv, L: $1, R: $3}
	}
|	BitExpr "MOD" BitExpr %prec mod
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Mod, L: $1, R: $3}
	}
|	BitExpr '^' BitExpr
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Xor, L: $1, R: $3}
	}
|	SimpleExpr

SimpleExpr:
	SimpleIdent
|	Literal
|	'!' SimpleExpr %prec neg
	{
		$$ = &ast.UnaryOperationExpr{Op: opcode.Not, V: $2}
	}
|	'~'  SimpleExpr %prec neg
	{
		$$ = &ast.UnaryOperationExpr{Op: opcode.BitNeg, V: $2}
	}
|	'-' SimpleExpr %prec neg
	{
		$$ = &ast.UnaryOperationExpr{Op: opcode.Minus, V: $2}
	}
|	'+' SimpleExpr %prec neg
	{
		$$ = &ast.UnaryOperationExpr{Op: opcode.Plus, V: $2}
	}
|	not2 SimpleExpr %prec neg
	{
		$$ = &ast.UnaryOperationExpr{Op: opcode.Not, V: $2}
	}
|	'(' Expression ')' {
		startOffset := parser.startOffset(&yyS[yypt-1])
		endOffset := parser.endOffset(&yyS[yypt])
		expr := $2
		expr.SetText(parser.src[startOffset:endOffset])
		$$ = &ast.ParenthesesExpr{Expr: expr}
	}


SimpleIdent:
	Identifier
	{
		$$ = &ast.ColumnNameExpr{Name: &ast.ColumnName{
			Name: model.NewCIStr($1),
		}}
	}
|	Identifier '.' Identifier
	{
		$$ = &ast.ColumnNameExpr{Name: &ast.ColumnName{
			Table: model.NewCIStr($1),
			Name: model.NewCIStr($3),
		}}
	}
|	'.' Identifier '.' Identifier
	{
		$$ = &ast.ColumnNameExpr{Name: &ast.ColumnName{
			Table: model.NewCIStr($2),
			Name: model.NewCIStr($4),
		}}
	}
|	Identifier '.' Identifier '.' Identifier
	{
		$$ = &ast.ColumnNameExpr{Name: &ast.ColumnName{
			Schema: model.NewCIStr($1),
			Table: model.NewCIStr($3),
			Name: model.NewCIStr($5),
		}}
	}


Literal:
	"FALSE"
	{
		$$ = ast.NewValueExpr(false)
	}
|	"NULL"
	{
		$$ = ast.NewValueExpr(nil)
	}
|	"TRUE"
	{
		$$ = ast.NewValueExpr(true)
	}
|	floatLit
	{
		$$ = ast.NewValueExpr($1)
	}
|	decLit
	{
		$$ = ast.NewValueExpr($1)
	}
|	intLit
	{
		$$ = ast.NewValueExpr($1)
	}
|	StringLiteral %prec lowerThanStringLitToken
	{
		$$ = $1
	}

StringLiteral:
	stringLit
	{
		expr := ast.NewValueExpr($1)
		$$ = expr
	}
|	StringLiteral stringLit
	{
		valExpr := $1.(ast.ValueExpr)
		strLit := valExpr.GetString()
		expr := ast.NewValueExpr(strLit+$2)
		// Fix #4239, use first string literal as projection name.
		if valExpr.GetProjectionOffset() >= 0 {
			expr.SetProjectionOffset(valExpr.GetProjectionOffset())
		} else {
			expr.SetProjectionOffset(len(strLit))
		}
		$$ = expr
	}

logOr:
	pipesAsOr
|	"OR"

logAnd:
"&&" | "AND"

IsOrNotOp:
	"IS"
	{
		$$ = true
	}
|	"IS" "NOT"
	{
		$$ = false
	}

InOrNotOp:
	"IN"
	{
		$$ = true
	}
|	"NOT" "IN"
	{
		$$ = false
	}

CompareOp:
	">="
	{
		$$ = opcode.GE
	}
|	'>'
	{
		$$ = opcode.GT
	}
|	"<="
	{
		$$ = opcode.LE
	}
|	'<'
	{
		$$ = opcode.LT
	}
|	"!="
	{
		$$ = opcode.NE
	}
|	"<>"
	{
		$$ = opcode.NE
	}
|	"="
	{
		$$ = opcode.EQ
	}
|	"<=>"
	{
		$$ = opcode.NullEQ
	}