package logstorage

import (
	"sort"
	"strings"
)

type fieldsSet map[string]struct{}

func newFieldsSet() fieldsSet {
	return fieldsSet(map[string]struct{}{})
}

func (fs fieldsSet) reset() {
	clear(fs)
}

func (fs fieldsSet) String() string {
	a := fs.getAll()
	return "[" + strings.Join(a, ",") + "]"
}

func (fs fieldsSet) clone() fieldsSet {
	fsNew := newFieldsSet()
	for _, f := range fs.getAll() {
		fsNew.add(f)
	}
	return fsNew
}

func (fs fieldsSet) getAll() []string {
	a := make([]string, 0, len(fs))
	for f := range fs {
		a = append(a, f)
	}
	sort.Strings(a)
	return a
}

func (fs fieldsSet) contains(field string) bool {
	_, ok := fs[field]
	if !ok {
		_, ok = fs["*"]
	}
	return ok
}

func (fs fieldsSet) removeAll(fields []string) {
	for _, f := range fields {
		fs.remove(f)
	}
}

func (fs fieldsSet) remove(field string) {
	if field == "*" {
		fs.reset()
		return
	}
	if !fs.contains("*") {
		delete(fs, field)
	}
}

func (fs fieldsSet) addAll(fields []string) {
	for _, f := range fields {
		fs.add(f)
	}
}

func (fs fieldsSet) add(field string) {
	if fs.contains("*") {
		return
	}
	if field == "*" {
		fs.reset()
		fs["*"] = struct{}{}
		return
	}
	fs[field] = struct{}{}
}
