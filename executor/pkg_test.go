package executor

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&pkgTestSuite{})

type pkgTestSuite struct {
}

func (s *pkgTestSuite) TestMoveInfoSchemaToFront(c *C) {
	dbss := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"A", "B", "C", "INFORMATION_SCHEMA"},
		{"A", "B", "INFORMATION_SCHEMA", "a"},
		{"INFORMATION_SCHEMA"},
		{"A", "B", "C", "INFORMATION_SCHEMA", "a", "b"},
	}
	wanted := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"INFORMATION_SCHEMA", "A", "B", "C"},
		{"INFORMATION_SCHEMA", "A", "B", "a"},
		{"INFORMATION_SCHEMA"},
		{"INFORMATION_SCHEMA", "A", "B", "C", "a", "b"},
	}

	for _, dbs := range dbss {
		moveInfoSchemaToFront(dbs)
	}

	for i, dbs := range wanted {
		c.Check(len(dbss[i]), Equals, len(dbs))
		for j, db := range dbs {
			c.Check(dbss[i][j], Equals, db)
		}
	}
}
