package logstorage

// filterNot negates the filter.
//
// It is expressed as `NOT f` or `!f` in LogsQL.
type filterNot struct {
	f filter
}

func (fn *filterNot) String() string {
	s := fn.f.String()
	switch fn.f.(type) {
	case *filterAnd, *filterOr:
		s = "(" + s + ")"
	}
	return "!" + s
}

func (fn *filterNot) apply(bs *blockSearch, bm *bitmap) {
	// Minimize the number of rows to check by the filter by applying it
	// only to the rows, which match the bm, e.g. they may change the bm result.
	bmTmp := getBitmap(bm.bitsLen)
	bmTmp.copyFrom(bm)
	fn.f.apply(bs, bmTmp)
	bm.andNot(bmTmp)
	putBitmap(bmTmp)
}
