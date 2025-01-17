package logstorage

import (
	"slices"
	"strconv"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type statsCountEmpty struct {
	fields       []string
	containsStar bool
}

func (sc *statsCountEmpty) String() string {
	return "count_empty(" + fieldNamesString(sc.fields) + ")"
}

func (sc *statsCountEmpty) neededFields() []string {
	return sc.fields
}

func (sc *statsCountEmpty) newStatsProcessor() (statsProcessor, int) {
	scp := &statsCountEmptyProcessor{
		sc: sc,
	}
	return scp, int(unsafe.Sizeof(*scp))
}

type statsCountEmptyProcessor struct {
	sc *statsCountEmpty

	rowsCount uint64
}

func (scp *statsCountEmptyProcessor) updateStatsForAllRows(br *blockResult) int {
	fields := scp.sc.fields
	if scp.sc.containsStar {
		bm := getBitmap(len(br.timestamps))
		bm.setBits()
		for _, c := range br.getColumns() {
			values := c.getValues(br)
			bm.forEachSetBit(func(idx int) bool {
				return values[idx] == ""
			})
		}
		scp.rowsCount += uint64(bm.onesCount())
		putBitmap(bm)
		return 0
	}
	if len(fields) == 1 {
		// Fast path for count_empty(single_column)
		c := br.getColumnByName(fields[0])
		if c.isConst {
			if c.encodedValues[0] == "" {
				scp.rowsCount += uint64(len(br.timestamps))
			}
			return 0
		}
		if c.isTime {
			return 0
		}
		switch c.valueType {
		case valueTypeString:
			for _, v := range c.encodedValues {
				if v == "" {
					scp.rowsCount++
				}
			}
			return 0
		case valueTypeDict:
			zeroDictIdx := slices.Index(c.dictValues, "")
			if zeroDictIdx < 0 {
				return 0
			}
			for _, v := range c.encodedValues {
				if int(v[0]) == zeroDictIdx {
					scp.rowsCount++
				}
			}
			return 0
		case valueTypeUint8, valueTypeUint16, valueTypeUint32, valueTypeUint64, valueTypeFloat64, valueTypeIPv4, valueTypeTimestampISO8601:
			return 0
		default:
			logger.Panicf("BUG: unknown valueType=%d", c.valueType)
			return 0
		}
	}

	// Slow path - count rows containing empty value for all the fields enumerated inside count_empty().
	bm := getBitmap(len(br.timestamps))
	defer putBitmap(bm)

	bm.setBits()
	for _, f := range fields {
		c := br.getColumnByName(f)
		if c.isConst {
			if c.encodedValues[0] == "" {
				scp.rowsCount += uint64(len(br.timestamps))
				return 0
			}
			continue
		}
		if c.isTime {
			return 0
		}
		switch c.valueType {
		case valueTypeString:
			bm.forEachSetBit(func(i int) bool {
				return c.encodedValues[i] == ""
			})
		case valueTypeDict:
			if !slices.Contains(c.dictValues, "") {
				return 0
			}
			bm.forEachSetBit(func(i int) bool {
				dictIdx := c.encodedValues[i][0]
				return c.dictValues[dictIdx] == ""
			})
		case valueTypeUint8, valueTypeUint16, valueTypeUint32, valueTypeUint64, valueTypeFloat64, valueTypeIPv4, valueTypeTimestampISO8601:
			return 0
		default:
			logger.Panicf("BUG: unknown valueType=%d", c.valueType)
			return 0
		}
	}

	scp.rowsCount += uint64(bm.onesCount())
	return 0
}

func (scp *statsCountEmptyProcessor) updateStatsForRow(br *blockResult, rowIdx int) int {
	fields := scp.sc.fields
	if scp.sc.containsStar {
		for _, c := range br.getColumns() {
			if v := c.getValueAtRow(br, rowIdx); v != "" {
				return 0
			}
		}
		scp.rowsCount++
		return 0
	}
	if len(fields) == 1 {
		// Fast path for count_empty(single_column)
		c := br.getColumnByName(fields[0])
		if c.isConst {
			if c.encodedValues[0] == "" {
				scp.rowsCount++
			}
			return 0
		}
		if c.isTime {
			return 0
		}
		switch c.valueType {
		case valueTypeString:
			if v := c.encodedValues[rowIdx]; v == "" {
				scp.rowsCount++
			}
			return 0
		case valueTypeDict:
			dictIdx := c.encodedValues[rowIdx][0]
			if v := c.dictValues[dictIdx]; v == "" {
				scp.rowsCount++
			}
			return 0
		case valueTypeUint8, valueTypeUint16, valueTypeUint32, valueTypeUint64, valueTypeFloat64, valueTypeIPv4, valueTypeTimestampISO8601:
			return 0
		default:
			logger.Panicf("BUG: unknown valueType=%d", c.valueType)
			return 0
		}
	}

	// Slow path - count the row at rowIdx if at least a single field enumerated inside count() is non-empty
	for _, f := range fields {
		c := br.getColumnByName(f)
		if v := c.getValueAtRow(br, rowIdx); v != "" {
			return 0
		}
	}
	scp.rowsCount++
	return 0
}

func (scp *statsCountEmptyProcessor) mergeState(sfp statsProcessor) {
	src := sfp.(*statsCountEmptyProcessor)
	scp.rowsCount += src.rowsCount
}

func (scp *statsCountEmptyProcessor) finalizeStats() string {
	return strconv.FormatUint(scp.rowsCount, 10)
}

func parseStatsCountEmpty(lex *lexer) (*statsCountEmpty, error) {
	fields, err := parseFieldNamesForStatsFunc(lex, "count_empty")
	if err != nil {
		return nil, err
	}
	sc := &statsCountEmpty{
		fields:       fields,
		containsStar: slices.Contains(fields, "*"),
	}
	return sc, nil
}
