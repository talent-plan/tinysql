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

package parser

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"unicode"

	"tinysql/parser/ast"
	"tinysql/parser/mysql"
	"tinysql/parser/terror"

	"github.com/pingcap/errors"
)

var (
	// ErrSyntax returns for sql syntax error.
	ErrSyntax = terror.ClassParser.NewStdErr(mysql.ErrSyntax, mysql.MySQLErrName[mysql.ErrSyntax])
	// ErrParse returns for sql parse error.
	ErrParse = terror.ClassParser.NewStdErr(mysql.ErrParse, mysql.MySQLErrName[mysql.ErrParse])
	// ErrUnknownCharacterSet returns for no character set found error.
	ErrUnknownCharacterSet = terror.ClassParser.NewStdErr(mysql.ErrUnknownCharacterSet, mysql.MySQLErrName[mysql.ErrUnknownCharacterSet])
	// ErrInvalidYearColumnLength returns for illegal column length for year type.
	ErrInvalidYearColumnLength = terror.ClassParser.NewStdErr(mysql.ErrInvalidYearColumnLength, mysql.MySQLErrName[mysql.ErrInvalidYearColumnLength])
	// ErrWrongArguments returns for illegal argument.
	ErrWrongArguments = terror.ClassParser.NewStdErr(mysql.ErrWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	// ErrWrongFieldTerminators returns for illegal field terminators.
	ErrWrongFieldTerminators = terror.ClassParser.NewStdErr(mysql.ErrWrongFieldTerminators, mysql.MySQLErrName[mysql.ErrWrongFieldTerminators])
	// ErrTooBigDisplayWidth returns for data display width exceed limit .
	ErrTooBigDisplayWidth = terror.ClassParser.NewStdErr(mysql.ErrTooBigDisplaywidth, mysql.MySQLErrName[mysql.ErrTooBigDisplaywidth])
	// ErrTooBigPrecision returns for data precision exceed limit.
	ErrTooBigPrecision = terror.ClassParser.NewStdErr(mysql.ErrTooBigPrecision, mysql.MySQLErrName[mysql.ErrTooBigPrecision])
	// ErrUnknownAlterLock returns for no alter lock type found error.
	ErrUnknownAlterLock = terror.ClassParser.NewStdErr(mysql.ErrUnknownAlterLock, mysql.MySQLErrName[mysql.ErrUnknownAlterLock])
	// ErrUnknownAlterAlgorithm returns for no alter algorithm found error.
	ErrUnknownAlterAlgorithm = terror.ClassParser.NewStdErr(mysql.ErrUnknownAlterAlgorithm, mysql.MySQLErrName[mysql.ErrUnknownAlterAlgorithm])
	// SpecFieldPattern special result field pattern
	SpecFieldPattern = regexp.MustCompile(`(\/\*!(M?[0-9]{5,6})?|\*\/)`)
	specCodePattern  = regexp.MustCompile(`\/\*!(M?[0-9]{5,6})?([^*]|\*+[^*/])*\*+\/`)
	specCodeStart    = regexp.MustCompile(`^\/\*!(M?[0-9]{5,6})?[ \t]*`)
	specCodeEnd      = regexp.MustCompile(`[ \t]*\*\/$`)
	// SpecVersionCodePattern is a pattern for special comments with version.
	SpecVersionCodePattern = regexp.MustCompile(`\/\*T![0-9]{5,6}([^*]|\*+[^*/])*\*+\/`)
	specVersionCodeStart   = regexp.MustCompile(`^\/\*T![0-9]{5,6}[ \t]*`)
	specVersionCodeValue   = regexp.MustCompile(`[0-9]{5,6}`)
)

func init() {
	parserMySQLErrCodes := map[terror.ErrCode]struct{}{
		mysql.ErrSyntax:                  {},
		mysql.ErrParse:                   {},
		mysql.ErrUnknownCharacterSet:     {},
		mysql.ErrInvalidYearColumnLength: {},
		mysql.ErrWrongArguments:          {},
		mysql.ErrWrongFieldTerminators:   {},
		mysql.ErrTooBigDisplaywidth:      {},
		mysql.ErrUnknownAlterLock:        {},
		mysql.ErrUnknownAlterAlgorithm:   {},
		mysql.ErrTooBigPrecision:         {},
	}
	terror.ErrClassToMySQLCodes[terror.ClassParser] = parserMySQLErrCodes
}

// TrimComment trim comment for special comment code of MySQL.
func TrimComment(txt string) string {
	txt = specCodeStart.ReplaceAllString(txt, "")
	return specCodeEnd.ReplaceAllString(txt, "")
}

func TrimCodeVersionComment(txt string) string {
	txt = specVersionCodeStart.ReplaceAllString(txt, "")
	return specCodeEnd.ReplaceAllString(txt, "")
}

// Parser represents a parser instance. Some temporary objects are stored in it to reduce object allocation during Parse function.
type Parser struct {
	charset   string
	collation string
	result    []ast.StmtNode
	src       string
	lexer     Scanner

	// the following fields are used by yyParse to reduce allocation.
	cache  []yySymType
	yylval yySymType
	yyVAL  *yySymType
}

type stmtTexter interface {
	stmtText() string
}

// New returns a Parser object.
func New() *Parser {
	if ast.NewValueExpr == nil {
		panic("no parser driver (forgotten import?) https://github.com/pingcap/tidb/parser/issues/43")
	}

	return &Parser{
		cache: make([]yySymType, 200),
	}
}

// Parse parses a query string to raw ast.StmtNode.
// If charset or collation is "", default charset and collation will be used.
func (parser *Parser) Parse(sql, charset, collation string) (stmt []ast.StmtNode, warns []error, err error) {
	if charset == "" {
		charset = mysql.DefaultCharset
	}
	if collation == "" {
		collation = mysql.DefaultCollationName
	}
	parser.charset = charset
	parser.collation = collation
	parser.src = sql
	parser.result = parser.result[:0]

	var l yyLexer
	parser.lexer.reset(sql)
	l = &parser.lexer
	yyParse(l, parser)

	warns, errs := l.Errors()
	if len(warns) > 0 {
		warns = append([]error(nil), warns...)
	} else {
		warns = nil
	}
	if len(errs) != 0 {
		return nil, warns, errors.Trace(errs[0])
	}
	for _, stmt := range parser.result {
		ast.SetFlag(stmt)
	}
	return parser.result, warns, nil
}

// ParseOneStmt parses a query and returns an ast.StmtNode.
// The query must have one statement, otherwise ErrSyntax is returned.
func (parser *Parser) ParseOneStmt(sql, charset, collation string) (ast.StmtNode, error) {
	stmts, _, err := parser.Parse(sql, charset, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(stmts) != 1 {
		return nil, ErrSyntax
	}
	ast.SetFlag(stmts[0])
	return stmts[0], nil
}

// ParseErrorWith returns "You have a syntax error near..." error message compatible with mysql.
func ParseErrorWith(errstr string, lineno int) error {
	return fmt.Errorf("near '%-.80s' at line %d", errstr, lineno)
}

func (parser *Parser) startOffset(v *yySymType) int {
	return v.offset
}

func (parser *Parser) endOffset(v *yySymType) int {
	offset := v.offset
	for offset > 0 && unicode.IsSpace(rune(parser.src[offset-1])) {
		offset--
	}
	return offset
}

func toInt(l yyLexer, lval *yySymType, str string) int {
	n, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		l.AppendError(l.Errorf("integer literal: %v", err))
		return int(unicode.ReplacementChar)
	}

	switch {
	case n <= math.MaxInt64:
		lval.item = int64(n)
	default:
		lval.item = n
	}
	return intLit
}

func toFloat(l yyLexer, lval *yySymType, str string) int {
	n, err := strconv.ParseFloat(str, 64)
	if err != nil {
		l.AppendError(l.Errorf("float literal: %v", err))
		return int(unicode.ReplacementChar)
	}

	lval.item = n
	return floatLit
}
