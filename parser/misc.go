// Copyright 2016 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/tidb/util/charset"
)

// CommentCodeVersion is used to track the highest version can be parsed in the comment with pattern /*T!00001 xxx */
type CommentCodeVersion int

const (
	CommentCodeNoVersion  CommentCodeVersion = iota
	CommentCodeAutoRandom CommentCodeVersion = 40000

	CommentCodeCurrentVersion
)

func (ccv CommentCodeVersion) String() string {
	return fmt.Sprintf("%05d", ccv)
}

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentChar(ch rune) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || isIdentExtend(ch)
}

func isIdentExtend(ch rune) bool {
	return ch >= 0x80 && ch <= '\uffff'
}

func isUserVarChar(ch rune) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || ch == '.' || isIdentExtend(ch)
}

type trieNode struct {
	childs [256]*trieNode
	token  int
	fn     func(s *Scanner) (int, Pos, string)
}

var ruleTable trieNode

func initTokenByte(c byte, tok int) {
	if ruleTable.childs[c] == nil {
		ruleTable.childs[c] = &trieNode{}
	}
	ruleTable.childs[c].token = tok
}

func initTokenString(str string, tok int) {
	node := &ruleTable
	for _, c := range str {
		if node.childs[c] == nil {
			node.childs[c] = &trieNode{}
		}
		node = node.childs[c]
	}
	node.token = tok
}

func initTokenFunc(str string, fn func(s *Scanner) (int, Pos, string)) {
	for i := 0; i < len(str); i++ {
		c := str[i]
		if ruleTable.childs[c] == nil {
			ruleTable.childs[c] = &trieNode{}
		}
		ruleTable.childs[c].fn = fn
	}
	return
}

func init() {
	// invalid is a special token defined in parser.y, when parser meet
	// this token, it will throw an error.
	// set root trie node's token to invalid, so when input match nothing
	// in the trie, invalid will be the default return token.
	ruleTable.token = invalid
	initTokenByte('*', int('*'))
	initTokenByte('/', int('/'))
	initTokenByte('+', int('+'))
	initTokenByte('>', int('>'))
	initTokenByte('<', int('<'))
	initTokenByte('(', int('('))
	initTokenByte(')', int(')'))
	initTokenByte('[', int('['))
	initTokenByte(']', int(']'))
	initTokenByte(';', int(';'))
	initTokenByte(',', int(','))
	initTokenByte('&', int('&'))
	initTokenByte('%', int('%'))
	initTokenByte(':', int(':'))
	initTokenByte('|', int('|'))
	initTokenByte('!', int('!'))
	initTokenByte('^', int('^'))
	initTokenByte('~', int('~'))
	initTokenByte('\\', int('\\'))
	initTokenByte('=', eq)
	initTokenByte('{', int('{'))
	initTokenByte('}', int('}'))

	initTokenString("||", pipes)
	initTokenString("&&", andand)
	initTokenString("&^", andnot)
	initTokenString(":=", assignmentEq)
	initTokenString("<=>", nulleq)
	initTokenString(">=", ge)
	initTokenString("<=", le)
	initTokenString("!=", neq)
	initTokenString("<>", neqSynonym)
	initTokenString("<<", lsh)
	initTokenString(">>", rsh)
	initTokenString("\\N", null)

	initTokenFunc("/", startWithSlash)
	initTokenFunc("-", startWithDash)
	initTokenFunc(".", startWithDot)
	initTokenFunc("_$ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", scanIdentifier)
	initTokenFunc("`", scanQuotedIdent)
	initTokenFunc("0123456789", startWithNumber)
	initTokenFunc("'\"", startString)
}

var tokenMap = map[string]int{

	"ALL":                     all,
	"ALTER":                   alter,
	"AND":                     and,
	"ANY":                     any,
	"AS":                      as,
	"ASC":                     asc,
	"ASCII":                   ascii,
	"AUTO_INCREMENT":          autoIncrement,
	"AVG":                     avg,
	"BEGIN":                   begin,
	"BETWEEN":                 between,
	"BIGINT":                  bigIntType,
	"BINARY":                  binaryType,
	"BINLOG":                  binlog,
	"BIT":                     bitType,
	"BIT_AND":                 bitAnd,
	"BIT_OR":                  bitOr,
	"BIT_XOR":                 bitXor,
	"BOOL":                    boolType,
	"BOOLEAN":                 booleanType,
	"BOTH":                    both,
	"BY":                      by,
	"BYTE":                    byteType,
	"CASCADE":                 cascade,
	"CASCADED":                cascaded,
	"CHAR":                    charType,
	"CHARACTER":               character,
	"CHARSET":                 charsetKwd,
	"COLUMN":                  column,
	"COLUMNS":                 columns,
	"COMMIT":                  commit,
	"COMMITTED":               committed,
	"CONNECTION":              connection,
	"CONSISTENT":              consistent,
	"CONSTRAINT":              constraint,
	"COUNT":                   count,
	"CREATE":                  create,
	"CROSS":                   cross,
	"DATABASE":                database,
	"DATABASES":               databases,
	"DATE":                    dateType,
	"DEC":                     decimalType,
	"DECIMAL":                 decimalType,
	"DEFAULT":                 defaultKwd,
	"DELETE":                  deleteKwd,
	"DESC":                    desc,
	"DESCRIBE":                describe,
	"DISABLE":                 disable,
	"DISTINCT":                distinct,
	"DIV":                     div,
	"DO":                      do,
	"DOUBLE":                  doubleType,
	"DROP":                    drop,
	"DUAL":                    dual,
	"DUPLICATE":               duplicate,
	"ELSE":                    elseKwd,
	"EXACT":                   exact,
	"EXCLUSIVE":               exclusive,
	"EXCEPT":                  except,
	"EXECUTE":                 execute,
	"EXISTS":                  exists,
	"EXPIRE":                  expire,
	"EXTRACT":                 extract,
	"FALSE":                   falseKwd,
	"FLOAT":                   floatType,
	"FOR":                     forKwd,
	"FROM":                    from,
	"GLOBAL":                  global,
	"GRANT":                   grant,
	"GRANTS":                  grants,
	"GROUP":                   group,
	"HASH":                    hash,
	"HASH_AGG":                hintHASHAGG,
	"HASH_JOIN":               hintHJ,
	"HAVING":                  having,
	"HIGH_PRIORITY":           highPriority,
	"IDENTIFIED":              identified,
	"IF":                      ifKwd,
	"IMPORT":                  importKwd,
	"IN":                      in,
	"INCREMENT":               increment,
	"INDEX":                   index,
	"INDEXES":                 indexes,
	"INNER":                   inner,
	"INSERT":                  insert,
	"INT":                     intType,
	"IO":                      io,
	"INTEGER":                 integerType,
	"INTO":                    into,
	"IS":                      is,
	"JOIN":                    join,
	"JSON":                    jsonType,
	"KEY":                     key,
	"KEYS":                    keys,
	"LEFT":                    left,
	"LESS":                    less,
	"LIMIT":                   limit,
	"LIST":                    list,
	"LOAD":                    load,
	"LOCK":                    lock,
	"MAX":                     max,
	"MOD":                     mod,
	"MODE":                    mode,
	"MODIFY":                  modify,
	"NATURAL":                 natural,
	"NEXT_ROW_ID":             next_row_id,
	"NO":                      no,
	"NOT":                     not,
	"NULL":                    null,
	"NULLS":                   nulls,
	"NUMERIC":                 numericType,
	"OFFSET":                  offset,
	"ON":                      on,
	"ONLY":                    only,
	"OPTIMISTIC":              optimistic,
	"OPTIMIZE":                optimize,
	"OPTION":                  option,
	"OR":                      or,
	"ORDER":                   order,
	"OUTER":                   outer,
	"PARTIAL":                 partial,
	"PARTITION":               partition,
	"PARTITIONING":            partitioning,
	"PARTITIONS":              partitions,
	"PASSWORD":                password,
	"PREPARE":                 prepare,
	"PRIMARY":                 primary,
	"PRIVILEGES":              privileges,
	"PROCEDURE":               procedure,
	"RANGE":                   rangeKwd,
	"RECOVER":                 recover,
	"READ":                    read,
	"READ_CONSISTENT_REPLICA": hintReadConsistentReplica,
	"READ_FROM_STORAGE":       hintReadFromStorage,
	"REGIONS":                 regions,
	"REGION":                  region,
	"REMOVE":                  remove,
	"RENAME":                  rename,
	"REPEAT":                  repeat,
	"REPLICA":                 replica,
	"REPLICATION":             replication,
	"REQUIRE":                 require,
	"RESTRICT":                restrict,
	"RIGHT":                   right,
	"ROLE":                    role,
	"ROLLBACK":                rollback,
	"SELECT":                  selectKwd,
	"SERIAL":                  serial,
	"SERIALIZABLE":            serializable,
	"SESSION":                 session,
	"SET":                     set,
	"SHOW":                    show,
	"SHUTDOWN":                shutdown,
	"SOME":                    some,
	"SOURCE":                  source,
	"STORAGE":                 storage,
	"OPEN":                    open,
	"SUM":                     sum,
	"TABLE":                   tableKwd,
	"TABLES":                  tables,
	"TEXT":                    textType,
	"THAN":                    than,
	"THEN":                    then,
	"TO":                      to,
	"TOP":                     top,
	"TOPN":                    topn,
	"TRANSACTION":             transaction,
	"TRIGGER":                 trigger,
	"TRIGGERS":                triggers,
	"TRIM":                    trim,
	"TRUE":                    trueKwd,
	"UNION":                   union,
	"UNIQUE":                  unique,
	"UNLOCK":                  unlock,
	"UPDATE":                  update,
	"USE":                     use,
	"VALUES":                  values,
	"VIEW":                    view,
	"WHERE":                   where,
	"WITH":                    with,
	"XOR":                     xor,
}

// See https://dev.mysql.com/doc/refman/5.7/en/function-resolution.html for details
var btFuncTokenMap = map[string]int{
	"ADDDATE":     builtinAddDate,
	"BIT_AND":     builtinBitAnd,
	"BIT_OR":      builtinBitOr,
	"BIT_XOR":     builtinBitXor,
	"COUNT":       builtinCount,
	"EXTRACT":     builtinExtract,
	"MAX":         builtinMax,
	"MID":         builtinSubstring,
	"MIN":         builtinMin,
	"NOW":         builtinNow,
	"STD":         builtinStddevPop,
	"STDDEV":      builtinStddevPop,
	"STDDEV_POP":  builtinStddevPop,
	"STDDEV_SAMP": builtinStddevSamp,
	"SUM":         builtinSum,
	"TRIM":        builtinTrim,
}

// aliases are strings directly map to another string and use the same token.
var aliases = map[string]string{
	"SCHEMA":  "DATABASE",
	"SCHEMAS": "DATABASES",
	"DEC":     "DECIMAL",
}

func (s *Scanner) isTokenIdentifier(lit string, offset int) int {
	// An identifier before or after '.' means it is part of a qualified identifier.
	// We do not parse it as keyword.
	if s.r.peek() == '.' {
		return 0
	}
	if offset > 0 && s.r.s[offset-1] == '.' {
		return 0
	}
	buf := &s.buf
	buf.Reset()
	buf.Grow(len(lit))
	data := buf.Bytes()[:len(lit)]
	for i := 0; i < len(lit); i++ {
		if lit[i] >= 'a' && lit[i] <= 'z' {
			data[i] = lit[i] + 'A' - 'a'
		} else {
			data[i] = lit[i]
		}
	}

	checkBtFuncToken := false
	if s.r.peek() == '(' {
		checkBtFuncToken = true
	}
	if checkBtFuncToken {
		if tok := btFuncTokenMap[string(data)]; tok != 0 {
			return tok
		}
	}
	tok, _ := tokenMap[string(data)]
	return tok
}

func handleIdent(lval *yySymType) int {
	s := lval.ident
	// A character string literal may have an optional character set introducer and COLLATE clause:
	// [_charset_name]'string' [COLLATE collation_name]
	// See https://dev.mysql.com/doc/refman/5.7/en/charset-literal.html
	if !strings.HasPrefix(s, "_") {
		return identifier
	}
	cs, _, err := charset.GetCharsetInfo(s[1:])
	if err != nil {
		return identifier
	}
	lval.ident = cs
	return underscoreCS
}
