package logstorage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// filterStringRange matches tie given string range [minValue..maxValue)
//
// Note that the minValue is included in the range, while the maxValue isn't included in the range.
// This simplifies querying distincts log sets with string_range(A, B), string_range(B, C), etc.
//
// Example LogsQL: `fieldName:string_range(minValue, maxValue)`
type filterStringRange struct {
	fieldName string
	minValue  string
	maxValue  string
}

func (fr *filterStringRange) String() string {
	return fmt.Sprintf("%sstring_range(%s, %s)", quoteFieldNameIfNeeded(fr.fieldName), quoteTokenIfNeeded(fr.minValue), quoteTokenIfNeeded(fr.maxValue))
}

func (fr *filterStringRange) apply(bs *blockSearch, bm *bitmap) {
	fieldName := fr.fieldName
	minValue := fr.minValue
	maxValue := fr.maxValue

	if minValue > maxValue {
		bm.resetBits()
		return
	}

	v := bs.csh.getConstColumnValue(fieldName)
	if v != "" {
		if !matchStringRange(v, minValue, maxValue) {
			bm.resetBits()
		}
		return
	}

	// Verify whether filter matches other columns
	ch := bs.csh.getColumnHeader(fieldName)
	if ch == nil {
		if !matchStringRange("", minValue, maxValue) {
			bm.resetBits()
		}
		return
	}

	switch ch.valueType {
	case valueTypeString:
		matchStringByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeDict:
		matchValuesDictByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeUint8:
		matchUint8ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeUint16:
		matchUint16ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeUint32:
		matchUint32ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeUint64:
		matchUint64ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeFloat64:
		matchFloat64ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeIPv4:
		matchIPv4ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeTimestampISO8601:
		matchTimestampISO8601ByStringRange(bs, ch, bm, minValue, maxValue)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
}

func matchTimestampISO8601ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return
	}

	bb := bbPool.Get()
	visitValues(bs, ch, bm, func(v string) bool {
		s := toTimestampISO8601StringExt(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
	bbPool.Put(bb)
}

func matchIPv4ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return
	}

	bb := bbPool.Get()
	visitValues(bs, ch, bm, func(v string) bool {
		s := toIPv4StringExt(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
	bbPool.Put(bb)
}

func matchFloat64ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) {
	if minValue > "9" || maxValue < "+" {
		bm.resetBits()
		return
	}

	bb := bbPool.Get()
	visitValues(bs, ch, bm, func(v string) bool {
		s := toFloat64StringExt(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
	bbPool.Put(bb)
}

func matchValuesDictByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) {
	bb := bbPool.Get()
	for i, v := range ch.valuesDict.values {
		if matchStringRange(v, minValue, maxValue) {
			bb.B = append(bb.B, byte(i))
		}
	}
	matchEncodedValuesDict(bs, ch, bm, bb.B)
	bbPool.Put(bb)
}

func matchStringByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) {
	visitValues(bs, ch, bm, func(v string) bool {
		return matchStringRange(v, minValue, maxValue)
	})
}

func matchUint8ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return
	}
	bb := bbPool.Get()
	visitValues(bs, ch, bm, func(v string) bool {
		s := toUint8String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
	bbPool.Put(bb)
}

func matchUint16ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return
	}
	bb := bbPool.Get()
	visitValues(bs, ch, bm, func(v string) bool {
		s := toUint16String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
	bbPool.Put(bb)
}

func matchUint32ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return
	}
	bb := bbPool.Get()
	visitValues(bs, ch, bm, func(v string) bool {
		s := toUint32String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
	bbPool.Put(bb)
}

func matchUint64ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return
	}
	bb := bbPool.Get()
	visitValues(bs, ch, bm, func(v string) bool {
		s := toUint64String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
	bbPool.Put(bb)
}

func matchStringRange(s, minValue, maxValue string) bool {
	return s >= minValue && s < maxValue
}
