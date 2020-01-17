package aggfuncs

import (
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/set"
)

type baseCount struct {
	baseAggFunc
}

type partialResult4Count = int64

func (e *baseCount) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4Count))
}

func (e *baseCount) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Count)(pr)
	*p = 0
}

func (e *baseCount) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Count)(pr)
	chk.AppendInt64(e.ordinal, *p)
	return nil
}

type countOriginal4Int struct {
	baseCount
}

func (e *countOriginal4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4Real struct {
	baseCount
}

func (e *countOriginal4Real) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4String struct {
	baseCount
}

func (e *countOriginal4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countPartial struct {
	baseCount
}

func (e *countPartial) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p += input
	}
	return nil
}

func (*countPartial) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4Count)(src), (*partialResult4Count)(dst)
	*p2 += *p1
	return nil
}

type countOriginalWithDistinct struct {
	baseCount
}

type partialResult4CountWithDistinct struct {
	count int64

	valSet set.StringSet
}

func (e *countOriginalWithDistinct) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4CountWithDistinct{
		count:  0,
		valSet: set.NewStringSet(),
	})
}

func (e *countOriginalWithDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountWithDistinct)(pr)
	p.count = 0
	p.valSet = set.NewStringSet()
}

func (e *countOriginalWithDistinct) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountWithDistinct)(pr)
	chk.AppendInt64(e.ordinal, p.count)
	return nil
}

func (e *countOriginalWithDistinct) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	p := (*partialResult4CountWithDistinct)(pr)

	encodedBytes := make([]byte, 0)
	buf := make([]byte, 8)

	for _, row := range rowsInGroup {
		var hasNull, isNull bool
		encodedBytes = encodedBytes[:0]

		for i := 0; i < len(e.args) && !hasNull; i++ {
			encodedBytes, isNull, err = e.evalAndEncode(sctx, e.args[i], row, buf, encodedBytes)
			if err != nil {
				return
			}
			if isNull {
				hasNull = true
				break
			}
		}
		encodedString := string(encodedBytes)
		if hasNull || p.valSet.Exist(encodedString) {
			continue
		}
		p.valSet.Insert(encodedString)
		p.count++
	}

	return nil
}

// evalAndEncode eval one row with an expression and encode value to bytes.
func (e *countOriginalWithDistinct) evalAndEncode(
	sctx sessionctx.Context, arg expression.Expression,
	row chunk.Row, buf, encodedBytes []byte,
) (_ []byte, isNull bool, err error) {
	switch tp := arg.GetType().EvalType(); tp {
	case types.ETInt:
		var val int64
		val, isNull, err = arg.EvalInt(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendInt64(encodedBytes, buf, val)
	case types.ETReal:
		var val float64
		val, isNull, err = arg.EvalReal(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendFloat64(encodedBytes, buf, val)
	case types.ETString:
		var val string
		val, isNull, err = arg.EvalString(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = codec.EncodeBytes(encodedBytes, hack.Slice(val))
	default:
		return nil, false, errors.Errorf("unsupported column type for encode %d", tp)
	}
	return encodedBytes, isNull, err
}

func appendInt64(encodedBytes, buf []byte, val int64) []byte {
	*(*int64)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:8]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendFloat64(encodedBytes, buf []byte, val float64) []byte {
	*(*float64)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:8]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}
