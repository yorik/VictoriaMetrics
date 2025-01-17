package logstorage

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLexer(t *testing.T) {
	f := func(s string, tokensExpected []string) {
		t.Helper()
		lex := newLexer(s)
		for _, tokenExpected := range tokensExpected {
			if lex.token != tokenExpected {
				t.Fatalf("unexpected token; got %q; want %q", lex.token, tokenExpected)
			}
			lex.nextToken()
		}
		if lex.token != "" {
			t.Fatalf("unexpected tail token: %q", lex.token)
		}
	}

	f("", nil)
	f("  ", nil)
	f("foo", []string{"foo"})
	f("тест123", []string{"тест123"})
	f("foo:bar", []string{"foo", ":", "bar"})
	f(` re   (  "тест(\":"  )  `, []string{"re", "(", `тест(":`, ")"})
	f(" `foo, bar`* AND baz:(abc or 'd\\'\"ЙЦУК `'*)", []string{"foo, bar", "*", "AND", "baz", ":", "(", "abc", "or", `d'"ЙЦУК ` + "`", "*", ")"})
	f(`_stream:{foo="bar",a=~"baz", b != 'cd',"d,}a"!~abc}`,
		[]string{"_stream", ":", "{", "foo", "=", "bar", ",", "a", "=~", "baz", ",", "b", "!=", "cd", ",", "d,}a", "!~", "abc", "}"})
}

func TestNewStreamFilterSuccess(t *testing.T) {
	f := func(s, resultExpected string) {
		t.Helper()
		sf, err := newStreamFilter(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		result := sf.String()
		if result != resultExpected {
			t.Fatalf("unexpected StreamFilter; got %s; want %s", result, resultExpected)
		}
	}

	f("{}", "{}")
	f(`{foo="bar"}`, `{foo="bar"}`)
	f(`{ "foo" =~ "bar.+" , baz!="a" or x="y"}`, `{foo=~"bar.+",baz!="a" or x="y"}`)
	f(`{"a b"='c}"d' OR de="aaa"}`, `{"a b"="c}\"d" or de="aaa"}`)
	f(`{a="b", c="d" or x="y"}`, `{a="b",c="d" or x="y"}`)
}

func TestNewStreamFilterFailure(t *testing.T) {
	f := func(s string) {
		t.Helper()
		sf, err := newStreamFilter(s)
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		if sf != nil {
			t.Fatalf("expecting nil sf; got %v", sf)
		}
	}

	f("")
	f("}")
	f("{")
	f("{foo")
	f("{foo}")
	f("{'foo")
	f("{foo=")
	f("{foo or bar}")
	f("{foo=bar")
	f("{foo=bar baz}")
	f("{foo='bar' baz='x'}")
}

func TestParseTimeDuration(t *testing.T) {
	f := func(s string, durationExpected time.Duration) {
		t.Helper()
		q, err := ParseQuery("_time:" + s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		ft, ok := q.f.(*filterTime)
		if !ok {
			t.Fatalf("unexpected filter; got %T; want *filterTime; filter: %s", q.f, q.f)
		}
		if ft.stringRepr != s {
			t.Fatalf("unexpected string represenation for filterTime; got %q; want %q", ft.stringRepr, s)
		}
		duration := time.Duration(ft.maxTimestamp - ft.minTimestamp)
		if duration != durationExpected {
			t.Fatalf("unexpected duration; got %s; want %s", duration, durationExpected)
		}
	}
	f("5m", 5*time.Minute)
	f("5m offset 1h", 5*time.Minute)
	f("5m offset -3.5h5m45s", 5*time.Minute)
	f("-5.5m", 5*time.Minute+30*time.Second)
	f("-5.5m offset 1d5m", 5*time.Minute+30*time.Second)
	f("3d2h12m34s45ms", 3*24*time.Hour+2*time.Hour+12*time.Minute+34*time.Second+45*time.Millisecond)
	f("3d2h12m34s45ms offset 10ms", 3*24*time.Hour+2*time.Hour+12*time.Minute+34*time.Second+45*time.Millisecond)
}

func TestParseTimeRange(t *testing.T) {
	f := func(s string, minTimestampExpected, maxTimestampExpected int64) {
		t.Helper()
		q, err := ParseQuery("_time:" + s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		ft, ok := q.f.(*filterTime)
		if !ok {
			t.Fatalf("unexpected filter; got %T; want *filterTime; filter: %s", q.f, q.f)
		}
		if ft.stringRepr != s {
			t.Fatalf("unexpected string represenation for filterTime; got %q; want %q", ft.stringRepr, s)
		}
		if ft.minTimestamp != minTimestampExpected {
			t.Fatalf("unexpected minTimestamp; got %s; want %s", timestampToString(ft.minTimestamp), timestampToString(minTimestampExpected))
		}
		if ft.maxTimestamp != maxTimestampExpected {
			t.Fatalf("unexpected maxTimestamp; got %s; want %s", timestampToString(ft.maxTimestamp), timestampToString(maxTimestampExpected))
		}
	}

	var minTimestamp, maxTimestamp int64

	// _time:YYYY -> _time:[YYYY, YYYY+1)
	minTimestamp = time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023", minTimestamp, maxTimestamp)
	f("2023Z", minTimestamp, maxTimestamp)

	// _time:YYYY-hh:mm -> _time:[YYYY-hh:mm, (YYYY+1)-hh:mm)
	minTimestamp = time.Date(2023, time.January, 1, 2, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2024, time.January, 1, 2, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02:00", minTimestamp, maxTimestamp)

	// _time:YYYY+hh:mm -> _time:[YYYY+hh:mm, (YYYY+1)+hh:mm)
	minTimestamp = time.Date(2022, time.December, 31, 22, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.December, 31, 22, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023+02:00", minTimestamp, maxTimestamp)

	// _time:YYYY-MM -> _time:[YYYY-MM, YYYY-MM+1)
	minTimestamp = time.Date(2023, time.February, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02", minTimestamp, maxTimestamp)
	f("2023-02Z", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-hh:mm -> _time:[YYYY-MM-hh:mm, (YYYY-MM+1)-hh:mm)
	minTimestamp = time.Date(2023, time.February, 1, 2, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 2, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02-02:00", minTimestamp, maxTimestamp)
	// March
	minTimestamp = time.Date(2023, time.March, 1, 2, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.April, 1, 2, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-03-02:00", minTimestamp, maxTimestamp)

	// _time:YYYY-MM+hh:mm -> _time:[YYYY-MM+hh:mm, (YYYY-MM+1)+hh:mm)
	minTimestamp = time.Date(2023, time.February, 28, 21, 35, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 31, 21, 35, 0, 0, time.UTC).UnixNano() - 1
	f("2023-03+02:25", minTimestamp, maxTimestamp)
	// February with timezone offset
	minTimestamp = time.Date(2023, time.January, 31, 21, 35, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.February, 28, 21, 35, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02+02:25", minTimestamp, maxTimestamp)
	// February with timezone offset at leap year
	minTimestamp = time.Date(2024, time.January, 31, 21, 35, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2024, time.February, 29, 21, 35, 0, 0, time.UTC).UnixNano() - 1
	f("2024-02+02:25", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DD
	minTimestamp = time.Date(2023, time.February, 12, 0, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.February, 13, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02-12", minTimestamp, maxTimestamp)
	f("2023-02-12Z", minTimestamp, maxTimestamp)
	// February 28
	minTimestamp = time.Date(2023, time.February, 28, 0, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02-28", minTimestamp, maxTimestamp)
	// January 31
	minTimestamp = time.Date(2023, time.January, 31, 0, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.February, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-01-31", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DD-hh:mm
	minTimestamp = time.Date(2023, time.January, 31, 2, 25, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.February, 1, 2, 25, 0, 0, time.UTC).UnixNano() - 1
	f("2023-01-31-02:25", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DD+hh:mm
	minTimestamp = time.Date(2023, time.February, 28, 21, 35, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 21, 35, 0, 0, time.UTC).UnixNano() - 1
	f("2023-03-01+02:25", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DDTHH
	minTimestamp = time.Date(2023, time.February, 28, 23, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02-28T23", minTimestamp, maxTimestamp)
	f("2023-02-28T23Z", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DDTHH-hh:mm
	minTimestamp = time.Date(2023, time.February, 28, 01, 25, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.February, 28, 02, 25, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02-27T23-02:25", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DDTHH+hh:mm
	minTimestamp = time.Date(2023, time.February, 28, 23, 35, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 00, 35, 0, 0, time.UTC).UnixNano() - 1
	f("2023-03-01T02+02:25", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DDTHH:MM
	minTimestamp = time.Date(2023, time.February, 28, 23, 59, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02-28T23:59", minTimestamp, maxTimestamp)
	f("2023-02-28T23:59Z", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DDTHH:MM-hh:mm
	minTimestamp = time.Date(2023, time.February, 28, 23, 59, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02-28T22:59-01:00", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DDTHH:MM+hh:mm
	minTimestamp = time.Date(2023, time.February, 28, 23, 59, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-03-01T00:59+01:00", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DDTHH:MM:SS-hh:mm
	minTimestamp = time.Date(2023, time.February, 28, 23, 59, 59, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02-28T23:59:59", minTimestamp, maxTimestamp)
	f("2023-02-28T23:59:59Z", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DDTHH:MM:SS-hh:mm
	minTimestamp = time.Date(2023, time.February, 28, 23, 59, 59, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-02-28T22:59:59-01:00", minTimestamp, maxTimestamp)

	// _time:YYYY-MM-DDTHH:MM:SS+hh:mm
	minTimestamp = time.Date(2023, time.February, 28, 23, 59, 59, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f("2023-03-01T00:59:59+01:00", minTimestamp, maxTimestamp)

	// _time:(start, end)
	minTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano() + 1
	maxTimestamp = time.Date(2023, time.April, 6, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f(`(2023-03-01,2023-04-06)`, minTimestamp, maxTimestamp)

	// _time:[start, end)
	minTimestamp = time.Date(2023, time.March, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.April, 6, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f(`[2023-03-01,2023-04-06)`, minTimestamp, maxTimestamp)

	// _time:(start, end]
	minTimestamp = time.Date(2023, time.March, 1, 21, 20, 0, 0, time.UTC).UnixNano() + 1
	maxTimestamp = time.Date(2023, time.April, 7, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f(`(2023-03-01T21:20,2023-04-06]`, minTimestamp, maxTimestamp)

	// _time:[start, end] with timezone
	minTimestamp = time.Date(2023, time.February, 28, 21, 40, 0, 0, time.UTC).UnixNano()
	maxTimestamp = time.Date(2023, time.April, 7, 0, 0, 0, 0, time.UTC).UnixNano() - 1
	f(`[2023-03-01+02:20,2023-04-06T23]`, minTimestamp, maxTimestamp)

	// _time:[start, end] with timezone and offset
	offset := int64(30*time.Minute + 5*time.Second)
	minTimestamp = time.Date(2023, time.February, 28, 21, 40, 0, 0, time.UTC).UnixNano() - offset
	maxTimestamp = time.Date(2023, time.April, 7, 0, 0, 0, 0, time.UTC).UnixNano() - 1 - offset
	f(`[2023-03-01+02:20,2023-04-06T23] offset 30m5s`, minTimestamp, maxTimestamp)
}

func TestParseFilterSequence(t *testing.T) {
	f := func(s, fieldNameExpected string, phrasesExpected []string) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fs, ok := q.f.(*filterSequence)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterSequence; filter: %s", q.f, q.f)
		}
		if fs.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", fs.fieldName, fieldNameExpected)
		}
		if !reflect.DeepEqual(fs.phrases, phrasesExpected) {
			t.Fatalf("unexpected phrases\ngot\n%q\nwant\n%q", fs.phrases, phrasesExpected)
		}
	}

	f(`seq()`, ``, nil)
	f(`foo:seq(foo)`, `foo`, []string{"foo"})
	f(`_msg:seq("foo bar,baz")`, `_msg`, []string{"foo bar,baz"})
	f(`seq(foo,bar-baz.aa"bb","c,)d")`, ``, []string{"foo", `bar-baz.aa"bb"`, "c,)d"})
}

func TestParseFilterIn(t *testing.T) {
	f := func(s, fieldNameExpected string, valuesExpected []string) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		f, ok := q.f.(*filterIn)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterIn; filter: %s", q.f, q.f)
		}
		if f.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", f.fieldName, fieldNameExpected)
		}
		if !reflect.DeepEqual(f.values, valuesExpected) {
			t.Fatalf("unexpected values\ngot\n%q\nwant\n%q", f.values, valuesExpected)
		}
	}

	f(`in()`, ``, nil)
	f(`foo:in(foo)`, `foo`, []string{"foo"})
	f(`:in("foo bar,baz")`, ``, []string{"foo bar,baz"})
	f(`ip:in(1.2.3.4, 5.6.7.8, 9.10.11.12)`, `ip`, []string{"1.2.3.4", "5.6.7.8", "9.10.11.12"})
	f(`foo-bar:in(foo,bar-baz.aa"bb","c,)d")`, `foo-bar`, []string{"foo", `bar-baz.aa"bb"`, "c,)d"})
}

func TestParseFilterIPv4Range(t *testing.T) {
	f := func(s, fieldNameExpected string, minValueExpected, maxValueExpected uint32) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fr, ok := q.f.(*filterIPv4Range)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterIPv4Range; filter: %s", q.f, q.f)
		}
		if fr.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", fr.fieldName, fieldNameExpected)
		}
		if fr.minValue != minValueExpected {
			t.Fatalf("unexpected minValue; got %08x; want %08x", fr.minValue, minValueExpected)
		}
		if fr.maxValue != maxValueExpected {
			t.Fatalf("unexpected maxValue; got %08x; want %08x", fr.maxValue, maxValueExpected)
		}
	}

	f(`ipv4_range(1.2.3.4, 5.6.7.8)`, ``, 0x01020304, 0x05060708)
	f(`_msg:ipv4_range("0.0.0.0", 255.255.255.255)`, `_msg`, 0, 0xffffffff)
	f(`ip:ipv4_range(1.2.3.0/24)`, `ip`, 0x01020300, 0x010203ff)
	f(`:ipv4_range("1.2.3.34/24")`, ``, 0x01020300, 0x010203ff)
	f(`ipv4_range("1.2.3.34/20")`, ``, 0x01020000, 0x01020fff)
	f(`ipv4_range("1.2.3.15/32")`, ``, 0x0102030f, 0x0102030f)
	f(`ipv4_range(1.2.3.34/0)`, ``, 0, 0xffffffff)
}

func TestParseFilterStringRange(t *testing.T) {
	f := func(s, fieldNameExpected, minValueExpected, maxValueExpected string) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fr, ok := q.f.(*filterStringRange)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterStringRange; filter: %s", q.f, q.f)
		}
		if fr.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", fr.fieldName, fieldNameExpected)
		}
		if fr.minValue != minValueExpected {
			t.Fatalf("unexpected minValue; got %q; want %q", fr.minValue, minValueExpected)
		}
		if fr.maxValue != maxValueExpected {
			t.Fatalf("unexpected maxValue; got %q; want %q", fr.maxValue, maxValueExpected)
		}
	}

	f("string_range(foo, bar)", ``, "foo", "bar")
	f(`abc:string_range("foo,bar", "baz) !")`, `abc`, `foo,bar`, `baz) !`)
}

func TestParseFilterRegexp(t *testing.T) {
	f := func(s, reExpected string) {
		t.Helper()
		q, err := ParseQuery("re(" + s + ")")
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fr, ok := q.f.(*filterRegexp)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterRegexp; filter: %s", q.f, q.f)
		}
		if reString := fr.re.String(); reString != reExpected {
			t.Fatalf("unexpected regexp; got %q; want %q", reString, reExpected)
		}
	}

	f(`""`, ``)
	f(`foo`, `foo`)
	f(`"foo.+|bar.*"`, `foo.+|bar.*`)
	f(`"foo(bar|baz),x[y]"`, `foo(bar|baz),x[y]`)
}

func TestParseAnyCaseFilterPhrase(t *testing.T) {
	f := func(s, fieldNameExpected, phraseExpected string) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fp, ok := q.f.(*filterAnyCasePhrase)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterAnyCasePhrase; filter: %s", q.f, q.f)
		}
		if fp.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", fp.fieldName, fieldNameExpected)
		}
		if fp.phrase != phraseExpected {
			t.Fatalf("unexpected phrase; got %q; want %q", fp.phrase, phraseExpected)
		}
	}

	f(`i("")`, ``, ``)
	f(`i(foo)`, ``, `foo`)
	f(`abc-de.fg:i(foo-bar+baz)`, `abc-de.fg`, `foo-bar+baz`)
	f(`"abc-de.fg":i("foo-bar+baz")`, `abc-de.fg`, `foo-bar+baz`)
}

func TestParseAnyCaseFilterPrefix(t *testing.T) {
	f := func(s, fieldNameExpected, prefixExpected string) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fp, ok := q.f.(*filterAnyCasePrefix)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterAnyCasePrefix; filter: %s", q.f, q.f)
		}
		if fp.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", fp.fieldName, fieldNameExpected)
		}
		if fp.prefix != prefixExpected {
			t.Fatalf("unexpected prefix; got %q; want %q", fp.prefix, prefixExpected)
		}
	}

	f(`i(*)`, ``, ``)
	f(`i(""*)`, ``, ``)
	f(`i(foo*)`, ``, `foo`)
	f(`abc-de.fg:i(foo-bar+baz*)`, `abc-de.fg`, `foo-bar+baz`)
	f(`"abc-de.fg":i("foo-bar+baz"*)`, `abc-de.fg`, `foo-bar+baz`)
	f(`"abc-de.fg":i("foo-bar*baz *"*)`, `abc-de.fg`, `foo-bar*baz *`)
}

func TestParseFilterPhrase(t *testing.T) {
	f := func(s, fieldNameExpected, phraseExpected string) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fp, ok := q.f.(*filterPhrase)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterPhrase; filter: %s", q.f, q.f)
		}
		if fp.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", fp.fieldName, fieldNameExpected)
		}
		if fp.phrase != phraseExpected {
			t.Fatalf("unexpected prefix; got %q; want %q", fp.phrase, phraseExpected)
		}
	}

	f(`""`, ``, ``)
	f(`foo`, ``, `foo`)
	f(`abc-de.fg:foo-bar+baz`, `abc-de.fg`, `foo-bar+baz`)
	f(`"abc-de.fg":"foo-bar+baz"`, `abc-de.fg`, `foo-bar+baz`)
	f(`"abc-de.fg":"foo-bar*baz *"`, `abc-de.fg`, `foo-bar*baz *`)
	f(`"foo:bar*,( baz"`, ``, `foo:bar*,( baz`)
}

func TestParseFilterPrefix(t *testing.T) {
	f := func(s, fieldNameExpected, prefixExpected string) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fp, ok := q.f.(*filterPrefix)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterPrefix; filter: %s", q.f, q.f)
		}
		if fp.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", fp.fieldName, fieldNameExpected)
		}
		if fp.prefix != prefixExpected {
			t.Fatalf("unexpected prefix; got %q; want %q", fp.prefix, prefixExpected)
		}
	}

	f(`*`, ``, ``)
	f(`""*`, ``, ``)
	f(`foo*`, ``, `foo`)
	f(`abc-de.fg:foo-bar+baz*`, `abc-de.fg`, `foo-bar+baz`)
	f(`"abc-de.fg":"foo-bar+baz"*`, `abc-de.fg`, `foo-bar+baz`)
	f(`"abc-de.fg":"foo-bar*baz *"*`, `abc-de.fg`, `foo-bar*baz *`)
}

func TestParseRangeFilter(t *testing.T) {
	f := func(s, fieldNameExpected string, minValueExpected, maxValueExpected float64) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fr, ok := q.f.(*filterRange)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterIPv4Range; filter: %s", q.f, q.f)
		}
		if fr.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", fr.fieldName, fieldNameExpected)
		}
		if fr.minValue != minValueExpected {
			t.Fatalf("unexpected minValue; got %v; want %v", fr.minValue, minValueExpected)
		}
		if fr.maxValue != maxValueExpected {
			t.Fatalf("unexpected maxValue; got %v; want %v", fr.maxValue, maxValueExpected)
		}
	}

	f(`range[-1.234, +2e5]`, ``, -1.234, 2e5)
	f(`foo:range[-1.234e-5, 2e5]`, `foo`, -1.234e-5, 2e5)
	f(`range:range["-1.234e5", "-2e-5"]`, `range`, -1.234e5, -2e-5)

	f(`_msg:range[1, 2]`, `_msg`, 1, 2)
	f(`:range(1, 2)`, ``, math.Nextafter(1, inf), math.Nextafter(2, -inf))
	f(`range[1, 2)`, ``, 1, math.Nextafter(2, -inf))
	f(`range("1", 2]`, ``, math.Nextafter(1, inf), 2)

	f(`response_size:range[1KB, 10MiB]`, `response_size`, 1_000, 10*(1<<20))
	f(`response_size:range[1G, 10Ti]`, `response_size`, 1_000_000_000, 10*(1<<40))
	f(`response_size:range[10, inf]`, `response_size`, 10, inf)

	f(`duration:range[100ns, 1y2w2.5m3s5ms]`, `duration`, 100, 1*nsecsPerYear+2*nsecsPerWeek+2.5*nsecsPerMinute+3*nsecsPerSecond+5*nsecsPerMillisecond)
}

func TestParseQuerySuccess(t *testing.T) {
	f := func(s, resultExpected string) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		result := q.String()
		if result != resultExpected {
			t.Fatalf("unexpected result;\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}

	f("foo", "foo")
	f(":foo", "foo")
	f(`"":foo`, "foo")
	f(`"" bar`, `"" bar`)
	f(`!''`, `!""`)
	f(`foo:""`, `foo:""`)
	f(`!foo:""`, `!foo:""`)
	f(`not foo:""`, `!foo:""`)
	f(`not(foo)`, `!foo`)
	f(`not (foo)`, `!foo`)
	f(`not ( foo or bar )`, `!(foo or bar)`)
	f(`foo:!""`, `!foo:""`)
	f("_msg:foo", "foo")
	f("'foo:bar'", `"foo:bar"`)
	f("'!foo'", `"!foo"`)
	f("foo 'and' and bar", `foo "and" bar`)
	f("foo bar", "foo bar")
	f("foo and bar", "foo bar")
	f("foo AND bar", "foo bar")
	f("foo or bar", "foo or bar")
	f("foo OR bar", "foo or bar")
	f("not foo", "!foo")
	f("! foo", "!foo")
	f("not !`foo bar`", `"foo bar"`)
	f("foo or bar and not baz", "foo or bar !baz")
	f("'foo bar' !baz", `"foo bar" !baz`)
	f("foo:!bar", `!foo:bar`)
	f(`foo and bar and baz or x or y or z and zz`, `foo bar baz or x or y or z zz`)
	f(`foo and bar and (baz or x or y or z) and zz`, `foo bar (baz or x or y or z) zz`)
	f(`(foo or bar or baz) and x and y and (z or zz)`, `(foo or bar or baz) x y (z or zz)`)
	f(`(foo or bar or baz) and x and y and not (z or zz)`, `(foo or bar or baz) x y !(z or zz)`)
	f(`NOT foo AND bar OR baz`, `!foo bar or baz`)
	f(`NOT (foo AND bar) OR baz`, `!(foo bar) or baz`)
	f(`foo OR bar AND baz`, `foo or bar baz`)
	f(`(foo OR bar) AND baz`, `(foo or bar) baz`)

	// parens
	f(`foo:(bar baz or not :xxx)`, `foo:bar foo:baz or !foo:xxx`)
	f(`(foo:bar and (foo:baz or aa:bb) and xx) and y`, `foo:bar (foo:baz or aa:bb) xx y`)
	f("level:error and _msg:(a or b)", "level:error (a or b)")
	f("level: ( ((error or warn*) and re(foo))) (not (bar))", `(level:error or level:warn*) level:re("foo") !bar`)
	f("!(foo bar or baz and not aa*)", `!(foo bar or baz !aa*)`)

	// prefix search
	f(`'foo'* and (a:x* and x:* or y:i(""*)) and i("abc def"*)`, `foo* (a:x* x:* or y:i(*)) i("abc def"*)`)

	// This isn't a prefix search - it equals to `foo AND *`
	f(`foo *`, `foo *`)
	f(`"foo" *`, `foo *`)

	// empty filter
	f(`"" or foo:"" and not bar:""`, `"" or foo:"" !bar:""`)

	// _stream filters
	f(`_stream:{}`, ``)
	f(`_stream:{foo="bar", baz=~"x" OR or!="b", "x=},"="d}{"}`, `_stream:{foo="bar",baz=~"x" or "or"!="b","x=},"="d}{"}`)
	f(`_stream:{or=a or ","="b"}`, `_stream:{"or"="a" or ","="b"}`)
	f("_stream : { foo =  bar , }  ", `_stream:{foo="bar"}`)

	// _time filters
	f(`_time:[-5m,now)`, `_time:[-5m,now)`)
	f(`_time:(  now-1h  , now-5m34s5ms]`, `_time:(now-1h,now-5m34s5ms]`)
	f(`_time:[2023, 2023-01)`, `_time:[2023,2023-01)`)
	f(`_time:[2023-01-02, 2023-02-03T04)`, `_time:[2023-01-02,2023-02-03T04)`)
	f(`_time:[2023-01-02T04:05, 2023-02-03T04:05:06)`, `_time:[2023-01-02T04:05,2023-02-03T04:05:06)`)
	f(`_time:[2023-01-02T04:05:06Z, 2023-02-03T04:05:06.234Z)`, `_time:[2023-01-02T04:05:06Z,2023-02-03T04:05:06.234Z)`)
	f(`_time:[2023-01-02T04:05:06+02:30, 2023-02-03T04:05:06.234-02:45)`, `_time:[2023-01-02T04:05:06+02:30,2023-02-03T04:05:06.234-02:45)`)
	f(`_time:[2023-06-07T23:56:34.3456-02:30, now)`, `_time:[2023-06-07T23:56:34.3456-02:30,now)`)
	f(`_time:("2024-01-02+02:00", now)`, `_time:(2024-01-02+02:00,now)`)
	f(`_time:now`, `_time:now`)
	f(`_time:"now"`, `_time:now`)
	f(`_time:2024Z`, `_time:2024Z`)
	f(`_time:2024-02:30`, `_time:2024-02:30`)
	f(`_time:2024-01-02:30`, `_time:2024-01-02:30`)
	f(`_time:2024-01-02:30`, `_time:2024-01-02:30`)
	f(`_time:2024-01-02+03:30`, `_time:2024-01-02+03:30`)
	f(`_time:2024-01-02T10+03:30`, `_time:2024-01-02T10+03:30`)
	f(`_time:2024-01-02T10:20+03:30`, `_time:2024-01-02T10:20+03:30`)
	f(`_time:2024-01-02T10:20:40+03:30`, `_time:2024-01-02T10:20:40+03:30`)
	f(`_time:2024-01-02T10:20:40-03:30`, `_time:2024-01-02T10:20:40-03:30`)
	f(`_time:"2024-01-02T10:20:40Z"`, `_time:2024-01-02T10:20:40Z`)
	f(`_time:2023-01-02T04:05:06.789Z`, `_time:2023-01-02T04:05:06.789Z`)
	f(`_time:2023-01-02T04:05:06.789-02:30`, `_time:2023-01-02T04:05:06.789-02:30`)
	f(`_time:2023-01-02T04:05:06.789+02:30`, `_time:2023-01-02T04:05:06.789+02:30`)
	f(`_time:[1234567890, 1400000000]`, `_time:[1234567890,1400000000]`)
	f(`_time:2d3h5.5m3s45ms`, `_time:2d3h5.5m3s45ms`)
	f(`_time:2023-01-05 OFFSET 5m`, `_time:2023-01-05 offset 5m`)
	f(`_time:[2023-01-05, 2023-01-06] OFFset 5m`, `_time:[2023-01-05,2023-01-06] offset 5m`)
	f(`_time:[2023-01-05, 2023-01-06) OFFset 5m`, `_time:[2023-01-05,2023-01-06) offset 5m`)
	f(`_time:(2023-01-05, 2023-01-06] OFFset 5m`, `_time:(2023-01-05,2023-01-06] offset 5m`)
	f(`_time:(2023-01-05, 2023-01-06) OFFset 5m`, `_time:(2023-01-05,2023-01-06) offset 5m`)
	f(`_time:1h offset 5m`, `_time:1h offset 5m`)
	f(`_time:1h "offSet"`, `_time:1h "offSet"`) // "offset" is a search word, since it is quoted
	f(`_time:1h (Offset)`, `_time:1h "Offset"`) // "offset" is a search word, since it is in parens
	f(`_time:1h "and"`, `_time:1h "and"`)       // "and" is a search word, since it is quoted

	// reserved keywords
	f("and", `"and"`)
	f("and and or", `"and" "or"`)
	f("AnD", `"AnD"`)
	f("or", `"or"`)
	f("re 'and' `or` 'not'", `"re" "and" "or" "not"`)
	f("foo:and", `foo:"and"`)
	f("'re':or or x", `"re":"or" or x`)
	f(`"-"`, `"-"`)
	f(`"!"`, `"!"`)
	f(`"not"`, `"not"`)
	f(`''`, `""`)

	// reserved functions
	f("exact", `"exact"`)
	f("exact:a", `"exact":a`)
	f("exact-foo", `exact-foo`)
	f("a:exact", `a:"exact"`)
	f("a:exact-foo", `a:exact-foo`)
	f("exact-foo:b", `exact-foo:b`)
	f("i", `"i"`)
	f("i-foo", `i-foo`)
	f("a:i-foo", `a:i-foo`)
	f("i-foo:b", `i-foo:b`)
	f("in", `"in"`)
	f("in:a", `"in":a`)
	f("in-foo", `in-foo`)
	f("a:in", `a:"in"`)
	f("a:in-foo", `a:in-foo`)
	f("in-foo:b", `in-foo:b`)
	f("ipv4_range", `"ipv4_range"`)
	f("ipv4_range:a", `"ipv4_range":a`)
	f("ipv4_range-foo", `ipv4_range-foo`)
	f("a:ipv4_range", `a:"ipv4_range"`)
	f("a:ipv4_range-foo", `a:ipv4_range-foo`)
	f("ipv4_range-foo:b", `ipv4_range-foo:b`)
	f("len_range", `"len_range"`)
	f("len_range:a", `"len_range":a`)
	f("len_range-foo", `len_range-foo`)
	f("a:len_range", `a:"len_range"`)
	f("a:len_range-foo", `a:len_range-foo`)
	f("len_range-foo:b", `len_range-foo:b`)
	f("range", `"range"`)
	f("range:a", `"range":a`)
	f("range-foo", `range-foo`)
	f("a:range", `a:"range"`)
	f("a:range-foo", `a:range-foo`)
	f("range-foo:b", `range-foo:b`)
	f("re", `"re"`)
	f("re-bar", `re-bar`)
	f("a:re-bar", `a:re-bar`)
	f("re-bar:a", `re-bar:a`)
	f("seq", `"seq"`)
	f("seq-a", `seq-a`)
	f("x:seq-a", `x:seq-a`)
	f("seq-a:x", `seq-a:x`)
	f("string_range", `"string_range"`)
	f("string_range-a", `string_range-a`)
	f("x:string_range-a", `x:string_range-a`)
	f("string_range-a:x", `string_range-a:x`)

	// exact filter
	f("exact(foo)", `exact(foo)`)
	f("exact(foo*)", `exact(foo*)`)
	f("exact('foo bar),|baz')", `exact("foo bar),|baz")`)
	f("exact('foo bar),|baz'*)", `exact("foo bar),|baz"*)`)
	f(`exact(foo|b:ar)`, `exact("foo|b:ar")`)
	f(`foo:exact(foo|b:ar*)`, `foo:exact("foo|b:ar"*)`)

	// i filter
	f("i(foo)", `i(foo)`)
	f("i(foo*)", `i(foo*)`)
	f("i(`foo`* )", `i(foo*)`)
	f("i(' foo ) bar')", `i(" foo ) bar")`)
	f("i('foo bar'*)", `i("foo bar"*)`)
	f(`foo:i(foo:bar-baz|aa+bb)`, `foo:i("foo:bar-baz|aa+bb")`)

	// in filter
	f(`in()`, `in()`)
	f(`in(foo)`, `in(foo)`)
	f(`in(foo, bar)`, `in(foo,bar)`)
	f(`in("foo bar", baz)`, `in("foo bar",baz)`)
	f(`foo:in(foo-bar|baz)`, `foo:in("foo-bar|baz")`)

	// ipv4_range filter
	f(`ipv4_range(1.2.3.4, "5.6.7.8")`, `ipv4_range(1.2.3.4, 5.6.7.8)`)
	f(`foo:ipv4_range(1.2.3.4, "5.6.7.8" , )`, `foo:ipv4_range(1.2.3.4, 5.6.7.8)`)
	f(`ipv4_range(1.2.3.4)`, `ipv4_range(1.2.3.4, 1.2.3.4)`)
	f(`ipv4_range(1.2.3.4/20)`, `ipv4_range(1.2.0.0, 1.2.15.255)`)
	f(`ipv4_range(1.2.3.4,)`, `ipv4_range(1.2.3.4, 1.2.3.4)`)

	// len_range filter
	f(`len_range(10, 20)`, `len_range(10, 20)`)
	f(`foo:len_range("10", 20, )`, `foo:len_range(10, 20)`)
	f(`len_RANGe(10, inf)`, `len_range(10, inf)`)
	f(`len_range(10, +InF)`, `len_range(10, +InF)`)
	f(`len_range(10, 1_000_000)`, `len_range(10, 1_000_000)`)
	f(`len_range(0x10,0b100101)`, `len_range(0x10, 0b100101)`)
	f(`len_range(1.5KB, 22MB100KB)`, `len_range(1.5KB, 22MB100KB)`)

	// range filter
	f(`range(1.234, 5656.43454)`, `range(1.234, 5656.43454)`)
	f(`foo:range(-2343.344, 2343.4343)`, `foo:range(-2343.344, 2343.4343)`)
	f(`range(-1.234e-5  , 2.34E+3)`, `range(-1.234e-5, 2.34E+3)`)
	f(`range[123, 456)`, `range[123, 456)`)
	f(`range(123, 445]`, `range(123, 445]`)
	f(`range("1.234e-4", -23)`, `range(1.234e-4, -23)`)
	f(`range(1_000, 0o7532)`, `range(1_000, 0o7532)`)
	f(`range(0x1ff, inf)`, `range(0x1ff, inf)`)
	f(`range(-INF,+inF)`, `range(-INF, +inF)`)
	f(`range(1.5K, 22.5GiB)`, `range(1.5K, 22.5GiB)`)

	// re filter
	f("re('foo|ba(r.+)')", `re("foo|ba(r.+)")`)
	f("re(foo)", `re("foo")`)
	f(`foo:re(foo-bar|baz.)`, `foo:re("foo-bar|baz.")`)

	// seq filter
	f(`seq()`, `seq()`)
	f(`seq(foo)`, `seq(foo)`)
	f(`seq("foo, bar", baz, abc)`, `seq("foo, bar",baz,abc)`)
	f(`foo:seq(foo"bar-baz+aa, b)`, `foo:seq("foo\"bar-baz+aa",b)`)

	// string_range filter
	f(`string_range(foo, bar)`, `string_range(foo, bar)`)
	f(`foo:string_range("foo, bar", baz)`, `foo:string_range("foo, bar", baz)`)

	// reserved field names
	f(`"_stream"`, `_stream`)
	f(`"_time"`, `_time`)
	f(`"_msg"`, `_msg`)
	f(`_stream and _time or _msg`, `_stream _time or _msg`)

	// invalid rune
	f("\xff", `"\xff"`)

	// ip addresses in the query
	f("1.2.3.4 or ip:5.6.7.9", "1.2.3.4 or ip:5.6.7.9")

	// '-' and '.' chars in field name and search phrase
	f("trace-id.foo.bar:baz", `trace-id.foo.bar:baz`)
	f(`custom-Time:2024-01-02T03:04:05+08:00    fooBar OR !baz:xxx`, `custom-Time:"2024-01-02T03:04:05+08:00" fooBar or !baz:xxx`)
	f("foo-bar+baz*", `"foo-bar+baz"*`)
	f("foo- bar", `foo- bar`)
	f("foo -bar", `foo -bar`)
	f("foo!bar", `"foo!bar"`)
	f("foo:aa!bb:cc", `foo:"aa!bb:cc"`)
	f(`foo:bar:baz`, `foo:"bar:baz"`)
	f(`foo:(bar baz:xxx)`, `foo:bar foo:"baz:xxx"`)
	f(`foo:(_time:abc or not z)`, `foo:"_time:abc" or !foo:z`)
	f(`foo:(_msg:a :x _stream:{c="d"})`, `foo:"_msg:a" foo:x foo:"_stream:{c=\"d\"}"`)
	f(`:(_msg:a:b c)`, `"a:b" c`)
	f(`"foo"bar baz:"a'b"c`, `"\"foo\"bar" baz:"\"a'b\"c"`)

	// complex queries
	f(`_time:[-1h, now] _stream:{job="foo",env=~"prod|staging"} level:(error or warn*) and not "connection reset by peer"`,
		`_time:[-1h,now] _stream:{job="foo",env=~"prod|staging"} (level:error or level:warn*) !"connection reset by peer"`)
	f(`(_time:(2023-04-20, now] or _time:[-10m, -1m))
		and (_stream:{job="a"} or _stream:{instance!="b"})
		and (err* or ip:(ipv4_range(1.2.3.0, 1.2.3.255) and not 1.2.3.4))`,
		`(_time:(2023-04-20,now] or _time:[-10m,-1m)) (_stream:{job="a"} or _stream:{instance!="b"}) (err* or ip:ipv4_range(1.2.3.0, 1.2.3.255) !ip:1.2.3.4)`)

	// fields pipe
	f(`foo|fields *`, `foo | fields *`)
	f(`foo | fields bar`, `foo | fields bar`)
	f(`foo|FIELDS bar,Baz  , "a,b|c"`, `foo | fields bar, Baz, "a,b|c"`)
	f(`foo | Fields   x.y, "abc:z/a", _b$c`, `foo | fields x.y, "abc:z/a", "_b$c"`)
	f(`foo | fields "", a`, `foo | fields _msg, a`)

	// multiple fields pipes
	f(`foo | fields bar | fields baz, abc`, `foo | fields bar | fields baz, abc`)

	// copy and cp pipe
	f(`* | copy foo as bar`, `* | copy foo as bar`)
	f(`* | cp foo bar`, `* | copy foo as bar`)
	f(`* | COPY foo as bar, x y | Copy a as b`, `* | copy foo as bar, x as y | copy a as b`)

	// rename and mv pipe
	f(`* | rename foo as bar`, `* | rename foo as bar`)
	f(`* | mv foo bar`, `* | rename foo as bar`)
	f(`* | RENAME foo AS bar, x y | Rename a as b`, `* | rename foo as bar, x as y | rename a as b`)

	// delete, del and rm pipe
	f(`* | delete foo`, `* | delete foo`)
	f(`* | del foo`, `* | delete foo`)
	f(`* | rm foo`, `* | delete foo`)
	f(`* | DELETE foo, bar`, `* | delete foo, bar`)

	// limit and head pipe
	f(`foo | limit 10`, `foo | limit 10`)
	f(`foo | head 10`, `foo | limit 10`)
	f(`foo | HEAD 1_123_432`, `foo | limit 1123432`)
	f(`foo | head 10K`, `foo | limit 10000`)

	// multiple limit pipes
	f(`foo | limit 100 | limit 10 | limit 234`, `foo | limit 100 | limit 10 | limit 234`)

	// offset and skip pipe
	f(`foo | skip 10`, `foo | offset 10`)
	f(`foo | offset 10`, `foo | offset 10`)
	f(`foo | skip 12_345M`, `foo | offset 12345000000`)

	// multiple offset pipes
	f(`foo | offset 10 | offset 100`, `foo | offset 10 | offset 100`)

	// stats pipe count
	f(`* | STATS bY (foo, b.a/r, "b az",) count(*) XYz`, `* | stats by (foo, "b.a/r", "b az") count(*) as XYz`)
	f(`* | stats by() COUNT(x, 'a).b,c|d',) as qwert`, `* | stats count(x, "a).b,c|d") as qwert`)
	f(`* | stats count() x`, `* | stats count(*) as x`)
	f(`* | stats count(*) x`, `* | stats count(*) as x`)
	f(`* | stats count(foo,*,bar) x`, `* | stats count(*) as x`)
	f(`* | stats count('') foo`, `* | stats count(_msg) as foo`)
	f(`* | stats count(foo) ''`, `* | stats count(foo) as _msg`)

	// stats pipe count_empty
	f(`* | stats count_empty() x`, `* | stats count_empty(*) as x`)
	f(`* | stats by (x, y) count_empty(a,b,c) x`, `* | stats by (x, y) count_empty(a, b, c) as x`)

	// stats pipe sum
	f(`* | stats Sum(foo) bar`, `* | stats sum(foo) as bar`)
	f(`* | stats BY(x, y, ) SUM(foo,bar,) bar`, `* | stats by (x, y) sum(foo, bar) as bar`)
	f(`* | stats sum() x`, `* | stats sum(*) as x`)
	f(`* | stats sum(*) x`, `* | stats sum(*) as x`)
	f(`* | stats sum(foo,*,bar) x`, `* | stats sum(*) as x`)

	// stats pipe max
	f(`* | stats Max(foo) bar`, `* | stats max(foo) as bar`)
	f(`* | stats BY(x, y, ) MAX(foo,bar,) bar`, `* | stats by (x, y) max(foo, bar) as bar`)
	f(`* | stats max() x`, `* | stats max(*) as x`)
	f(`* | stats max(*) x`, `* | stats max(*) as x`)
	f(`* | stats max(foo,*,bar) x`, `* | stats max(*) as x`)

	// stats pipe min
	f(`* | stats Min(foo) bar`, `* | stats min(foo) as bar`)
	f(`* | stats BY(x, y, ) MIN(foo,bar,) bar`, `* | stats by (x, y) min(foo, bar) as bar`)
	f(`* | stats min() x`, `* | stats min(*) as x`)
	f(`* | stats min(*) x`, `* | stats min(*) as x`)
	f(`* | stats min(foo,*,bar) x`, `* | stats min(*) as x`)

	// stats pipe avg
	f(`* | stats Avg(foo) bar`, `* | stats avg(foo) as bar`)
	f(`* | stats BY(x, y, ) AVG(foo,bar,) bar`, `* | stats by (x, y) avg(foo, bar) as bar`)
	f(`* | stats avg() x`, `* | stats avg(*) as x`)
	f(`* | stats avg(*) x`, `* | stats avg(*) as x`)
	f(`* | stats avg(foo,*,bar) x`, `* | stats avg(*) as x`)

	// stats pipe count_uniq
	f(`* | stats count_uniq(foo) bar`, `* | stats count_uniq(foo) as bar`)
	f(`* | stats by(x, y) count_uniq(foo,bar) LiMit 10 As baz`, `* | stats by (x, y) count_uniq(foo, bar) limit 10 as baz`)
	f(`* | stats by(x) count_uniq(*) z`, `* | stats by (x) count_uniq(*) as z`)
	f(`* | stats by(x) count_uniq() z`, `* | stats by (x) count_uniq(*) as z`)
	f(`* | stats by(x) count_uniq(a,*,b) z`, `* | stats by (x) count_uniq(*) as z`)

	// stats pipe uniq_values
	f(`* | stats uniq_values(foo) bar`, `* | stats uniq_values(foo) as bar`)
	f(`* | stats uniq_values(foo) limit 10 bar`, `* | stats uniq_values(foo) limit 10 as bar`)
	f(`* | stats by(x, y) uniq_values(foo, bar) as baz`, `* | stats by (x, y) uniq_values(foo, bar) as baz`)
	f(`* | stats by(x) uniq_values(*) y`, `* | stats by (x) uniq_values(*) as y`)
	f(`* | stats by(x) uniq_values() limit 1_000 AS y`, `* | stats by (x) uniq_values(*) limit 1000 as y`)
	f(`* | stats by(x) uniq_values(a,*,b) y`, `* | stats by (x) uniq_values(*) as y`)

	// stats pipe values
	f(`* | stats values(foo) bar`, `* | stats values(foo) as bar`)
	f(`* | stats values(foo) limit 10 bar`, `* | stats values(foo) limit 10 as bar`)
	f(`* | stats by(x, y) values(foo, bar) as baz`, `* | stats by (x, y) values(foo, bar) as baz`)
	f(`* | stats by(x) values(*) y`, `* | stats by (x) values(*) as y`)
	f(`* | stats by(x) values() limit 1_000 AS y`, `* | stats by (x) values(*) limit 1000 as y`)
	f(`* | stats by(x) values(a,*,b) y`, `* | stats by (x) values(*) as y`)

	// stats pipe sum_len
	f(`* | stats Sum_len(foo) bar`, `* | stats sum_len(foo) as bar`)
	f(`* | stats BY(x, y, ) SUM_Len(foo,bar,) bar`, `* | stats by (x, y) sum_len(foo, bar) as bar`)
	f(`* | stats sum_len() x`, `* | stats sum_len(*) as x`)
	f(`* | stats sum_len(*) x`, `* | stats sum_len(*) as x`)
	f(`* | stats sum_len(foo,*,bar) x`, `* | stats sum_len(*) as x`)

	// stats pipe quantile
	f(`* | stats quantile(0, foo) bar`, `* | stats quantile(0, foo) as bar`)
	f(`* | stats quantile(1, foo) bar`, `* | stats quantile(1, foo) as bar`)
	f(`* | stats quantile(0.5, a, b, c) bar`, `* | stats quantile(0.5, a, b, c) as bar`)
	f(`* | stats quantile(0.99, *) bar`, `* | stats quantile(0.99, *) as bar`)
	f(`* | stats quantile(0.99, a, *, b) bar`, `* | stats quantile(0.99, *) as bar`)

	// stats pipe median
	f(`* | stats Median(foo) bar`, `* | stats median(foo) as bar`)
	f(`* | stats BY(x, y, ) MEDIAN(foo,bar,) bar`, `* | stats by (x, y) median(foo, bar) as bar`)
	f(`* | stats median() x`, `* | stats median(*) as x`)
	f(`* | stats median(*) x`, `* | stats median(*) as x`)
	f(`* | stats median(foo,*,bar) x`, `* | stats median(*) as x`)

	// stats pipe multiple funcs
	f(`* | stats count() "foo.bar:baz", count_uniq(a) bar`, `* | stats count(*) as "foo.bar:baz", count_uniq(a) as bar`)
	f(`* | stats by (x, y) count(*) foo, count_uniq(a,b) bar`, `* | stats by (x, y) count(*) as foo, count_uniq(a, b) as bar`)

	// stats pipe with grouping buckets
	f(`* | stats by(_time:1d, response_size:1_000KiB, request_duration:5s, foo) count() as foo`, `* | stats by (_time:1d, response_size:1_000KiB, request_duration:5s, foo) count(*) as foo`)
	f(`*|stats by(client_ip:/24, server_ip:/16) count() foo`, `* | stats by (client_ip:/24, server_ip:/16) count(*) as foo`)
	f(`* | stats by(_time:1d offset 2h) count() as foo`, `* | stats by (_time:1d offset 2h) count(*) as foo`)
	f(`* | stats by(_time:1d offset -2.5h5m) count() as foo`, `* | stats by (_time:1d offset -2.5h5m) count(*) as foo`)
	f(`* | stats by (_time:nanosecond) count() foo`, `* | stats by (_time:nanosecond) count(*) as foo`)
	f(`* | stats by (_time:microsecond) count() foo`, `* | stats by (_time:microsecond) count(*) as foo`)
	f(`* | stats by (_time:millisecond) count() foo`, `* | stats by (_time:millisecond) count(*) as foo`)
	f(`* | stats by (_time:second) count() foo`, `* | stats by (_time:second) count(*) as foo`)
	f(`* | stats by (_time:minute) count() foo`, `* | stats by (_time:minute) count(*) as foo`)
	f(`* | stats by (_time:hour) count() foo`, `* | stats by (_time:hour) count(*) as foo`)
	f(`* | stats by (_time:day) count() foo`, `* | stats by (_time:day) count(*) as foo`)
	f(`* | stats by (_time:week) count() foo`, `* | stats by (_time:week) count(*) as foo`)
	f(`* | stats by (_time:month) count() foo`, `* | stats by (_time:month) count(*) as foo`)
	f(`* | stats by (_time:year offset 6.5h) count() foo`, `* | stats by (_time:year offset 6.5h) count(*) as foo`)

	// sort pipe
	f(`* | sort`, `* | sort`)
	f(`* | sort desc`, `* | sort desc`)
	f(`* | sort by()`, `* | sort`)
	f(`* | sort bY (foo)`, `* | sort by (foo)`)
	f(`* | sORt bY (_time, _stream DEsc, host)`, `* | sort by (_time, _stream desc, host)`)
	f(`* | sort bY (foo desc, bar,) desc`, `* | sort by (foo desc, bar) desc`)
	f(`* | sort limit 10`, `* | sort limit 10`)
	f(`* | sort offset 20 limit 10`, `* | sort offset 20 limit 10`)
	f(`* | sort desc limit 10`, `* | sort desc limit 10`)
	f(`* | sort desc offset 20 limit 10`, `* | sort desc offset 20 limit 10`)
	f(`* | sort by (foo desc, bar) limit 10`, `* | sort by (foo desc, bar) limit 10`)
	f(`* | sort by (foo desc, bar) oFFset 20 limit 10`, `* | sort by (foo desc, bar) offset 20 limit 10`)
	f(`* | sort by (foo desc, bar) desc limit 10`, `* | sort by (foo desc, bar) desc limit 10`)
	f(`* | sort by (foo desc, bar) desc OFFSET 30 limit 10`, `* | sort by (foo desc, bar) desc offset 30 limit 10`)
	f(`* | sort by (foo desc, bar) desc limit 10 OFFSET 30`, `* | sort by (foo desc, bar) desc offset 30 limit 10`)

	// uniq pipe
	f(`* | uniq`, `* | uniq`)
	f(`* | uniq by()`, `* | uniq`)
	f(`* | uniq by(*)`, `* | uniq`)
	f(`* | uniq by(foo,*,bar)`, `* | uniq`)
	f(`* | uniq by(f1,f2)`, `* | uniq by (f1, f2)`)
	f(`* | uniq by(f1,f2) limit 10`, `* | uniq by (f1, f2) limit 10`)
	f(`* | uniq limit 10`, `* | uniq limit 10`)

	// multiple different pipes
	f(`* | fields foo, bar | limit 100 | stats by(foo,bar) count(baz) as qwert`, `* | fields foo, bar | limit 100 | stats by (foo, bar) count(baz) as qwert`)
	f(`* | skip 100 | head 20 | skip 10`, `* | offset 100 | limit 20 | offset 10`)
}

func TestParseQueryFailure(t *testing.T) {
	f := func(s string) {
		t.Helper()
		q, err := ParseQuery(s)
		if q != nil {
			t.Fatalf("expecting nil result; got %s", q)
		}
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
	}

	f("")
	f("|")
	f("foo|")
	f("foo|bar")
	f("foo and")
	f("foo OR ")
	f("not")
	f("NOT")
	f("not (abc")
	f("!")

	// invalid parens
	f("(")
	f("foo (bar ")
	f("(foo:'bar")

	// missing filter
	f(":")
	f(":  ")
	f("foo:  ")
	f("_msg :   ")
	f(`"":   `)

	// invalid quoted strings
	f(`"foo`)
	f(`'foo`)
	f("`foo")

	// invalid _stream filters
	f("_stream:")
	f("_stream:{")
	f("_stream:(")
	f("_stream:{foo")
	f("_stream:{foo}")
	f("_stream:{foo=")
	f("_stream:{foo='bar")
	f("_stream:{foo='bar}")
	f("_stream:{foo=bar or")
	f("_stream:{foo=bar or}")
	f("_stream:{foo=bar or baz}")
	f("_stream:{foo=bar baz x=y}")
	f("_stream:{foo=bar,")
	f("_stream:{foo=bar")
	f("_stream:foo")
	f("_stream:(foo)")
	f("_stream:[foo]")

	// invalid _time filters
	f("_time:")
	f("_time:[")
	f("_time:foo")
	f("_time:{}")
	f("_time:[foo,bar)")
	f("_time:(now)")
	f("_time:[now,")
	f("_time:(now, not now]")
	f("_time:(-5m, -1m}")
	f("_time:[-")
	f("_time:[now-foo,-bar]")
	f("_time:[2023-ab,2023]")
	f("_time:[fooo-02,2023]")
	f("_time:[2023-01-02T04:05:06+12,2023]")
	f("_time:[2023-01-02T04:05:06-12,2023]")
	f("_time:2023-01-02T04:05:06.789")
	f("_time:234foo")
	f("_time:5m offset")
	f("_time:10m offset foobar")

	// long query with error
	f(`very long query with error aaa ffdfd fdfdfd fdfd:( ffdfdfdfdfd`)

	// query with unexpected tail
	f(`foo | bar`)

	// unexpected comma
	f(`foo,bar`)
	f(`foo, bar`)
	f(`foo ,bar`)

	// unexpected token
	f(`[foo`)
	f(`foo]bar`)
	f(`foo] bar`)
	f(`foo ]bar`)
	f(`) foo`)
	f(`foo)bar`)

	// unknown function
	f(`unknown_function(foo)`)

	// invalid exact
	f(`exact(`)
	f(`exact(f, b)`)
	f(`exact(foo`)
	f(`exact(foo,`)
	f(`exact(foo bar)`)
	f(`exact(foo, bar`)
	f(`exact(foo,)`)

	// invalid i
	f(`i(`)
	f(`i(aa`)
	f(`i(aa, bb)`)
	f(`i(*`)
	f(`i(aaa*`)
	f(`i(a**)`)
	f(`i("foo`)
	f(`i(foo bar)`)

	// invalid in
	f(`in(`)
	f(`in(,)`)
	f(`in(f, b c)`)
	f(`in(foo`)
	f(`in(foo,`)
	f(`in(foo*)`)
	f(`in(foo, "bar baz"*)`)
	f(`in(foo, "bar baz"*, abc)`)
	f(`in(foo bar)`)
	f(`in(foo, bar`)

	// invalid ipv4_range
	f(`ipv4_range(`)
	f(`ipv4_range(foo,bar)`)
	f(`ipv4_range(1.2.3.4*)`)
	f(`ipv4_range("1.2.3.4"*)`)
	f(`ipv4_range(1.2.3.4`)
	f(`ipv4_range(1.2.3.4,`)
	f(`ipv4_range(1.2.3.4, 5.6.7)`)
	f(`ipv4_range(1.2.3.4, 5.6.7.8`)
	f(`ipv4_range(1.2.3.4, 5.6.7.8,`)
	f(`ipv4_range(1.2.3.4, 5.6.7.8,,`)
	f(`ipv4_range(1.2.3.4, 5.6.7.8,5.3.2.1)`)

	// invalid len_range
	f(`len_range(`)
	f(`len_range(1)`)
	f(`len_range(foo, bar)`)
	f(`len_range(1, bar)`)
	f(`len_range(1, 2`)
	f(`len_range(1.2, 3.4)`)

	// invalid range
	f(`range(`)
	f(`range(foo,bar)`)
	f(`range(1"`)
	f(`range(1,`)
	f(`range(1)`)
	f(`range(1,)`)
	f(`range(1,2,`)
	f(`range[1,foo)`)
	f(`range[1,2,3)`)
	f(`range(1)`)

	// invalid re
	f("re(")
	f("re(a, b)")
	f("foo:re(bar")
	f("re(`ab(`)")
	f(`re(a b)`)

	// invalid seq
	f(`seq(`)
	f(`seq(,)`)
	f(`seq(foo`)
	f(`seq(foo,`)
	f(`seq(foo*)`)
	f(`seq(foo*, bar)`)
	f(`seq(foo bar)`)
	f(`seq(foo, bar`)

	// invalid string_range
	f(`string_range(`)
	f(`string_range(,)`)
	f(`string_range(foo`)
	f(`string_range(foo,`)
	f(`string_range(foo*)`)
	f(`string_range(foo bar)`)
	f(`string_range(foo, bar`)
	f(`string_range(foo)`)
	f(`string_range(foo, bar, baz)`)

	// missing filter
	f(`| fields *`)

	// missing pipe keyword
	f(`foo |`)

	// unknown pipe keyword
	f(`foo | bar`)
	f(`foo | fields bar | baz`)

	// missing field in fields pipe
	f(`foo | fields`)
	f(`foo | fields ,`)
	f(`foo | fields bar,`)
	f(`foo | fields bar,,`)

	// invalid copy and cp pipe
	f(`foo | copy`)
	f(`foo | cp`)
	f(`foo | copy foo`)
	f(`foo | copy foo,`)
	f(`foo | copy foo,,`)

	// invalid rename and mv pipe
	f(`foo | rename`)
	f(`foo | mv`)
	f(`foo | rename foo`)
	f(`foo | rename foo,`)
	f(`foo | rename foo,,`)

	// invalid delete pipe
	f(`foo | delete`)
	f(`foo | del`)
	f(`foo | rm`)
	f(`foo | delete foo,`)
	f(`foo | delete foo,,`)

	// missing limit and head pipe value
	f(`foo | limit`)
	f(`foo | head`)

	// invalid limit pipe value
	f(`foo | limit bar`)
	f(`foo | limit -123`)

	// missing offset and skip pipe value
	f(`foo | offset`)
	f(`foo | skip`)

	// invalid offset pipe value
	f(`foo | offset bar`)
	f(`foo | offset -10`)

	// missing stats
	f(`foo | stats`)

	// invalid stats
	f(`foo | stats bar`)

	// invalid stats count
	f(`foo | stats count`)
	f(`foo | stats count(`)
	f(`foo | stats count bar`)
	f(`foo | stats count(bar`)
	f(`foo | stats count(bar)`)
	f(`foo | stats count() as`)
	f(`foo | stats count() as |`)

	// invalid stats count_empty
	f(`foo | stats count_empty`)
	f(`foo | stats count_empty() as`)
	f(`foo | stats count_empty() as |`)

	// invalid stats sum
	f(`foo | stats sum`)
	f(`foo | stats sum()`)

	// invalid stats max
	f(`foo | stats max`)
	f(`foo | stats max()`)

	// invalid stats min
	f(`foo | stats min`)
	f(`foo | stats min()`)

	// invalid stats avg
	f(`foo | stats avg`)
	f(`foo | stats avg()`)

	// invalid stats count_uniq
	f(`foo | stats count_uniq`)
	f(`foo | stats count_uniq()`)
	f(`foo | stats count_uniq() limit`)
	f(`foo | stats count_uniq() limit foo`)
	f(`foo | stats count_uniq() limit 0.5`)
	f(`foo | stats count_uniq() limit -1`)

	// invalid stats uniq_values
	f(`foo | stats uniq_values`)
	f(`foo | stats uniq_values()`)
	f(`foo | stats uniq_values() limit`)
	f(`foo | stats uniq_values(a) limit foo`)
	f(`foo | stats uniq_values(a) limit 0.5`)
	f(`foo | stats uniq_values(a) limit -1`)

	// invalid stats values
	f(`foo | stats values`)
	f(`foo | stats values()`)
	f(`foo | stats values() limit`)
	f(`foo | stats values(a) limit foo`)
	f(`foo | stats values(a) limit 0.5`)
	f(`foo | stats values(a) limit -1`)

	// invalid stats sum_len
	f(`foo | stats sum_len`)
	f(`foo | stats sum_len()`)

	// invalid stats quantile
	f(`foo | stats quantile`)
	f(`foo | stats quantile() foo`)
	f(`foo | stats quantile(bar, baz) foo`)
	f(`foo | stats quantile(0.5) foo`)
	f(`foo | stats quantile(-1, x) foo`)
	f(`foo | stats quantile(10, x) foo`)

	// invalid stats grouping fields
	f(`foo | stats by(foo:bar) count() baz`)
	f(`foo | stats by(foo:/bar) count() baz`)
	f(`foo | stats by(foo:-1h) count() baz`)
	f(`foo | stats by (foo:1h offset) count() baz`)
	f(`foo | stats by (foo:1h offset bar) count() baz`)

	// invalid stats by clause
	f(`foo | stats by`)
	f(`foo | stats by bar`)
	f(`foo | stats by(`)
	f(`foo | stats by(bar`)
	f(`foo | stats by(bar,`)
	f(`foo | stats by(bar)`)

	// invalid sort pipe
	f(`foo | sort bar`)
	f(`foo | sort by`)
	f(`foo | sort by(`)
	f(`foo | sort by(baz`)
	f(`foo | sort by(baz,`)
	f(`foo | sort by(bar) foo`)
	f(`foo | sort by(bar) limit`)
	f(`foo | sort by(bar) limit foo`)
	f(`foo | sort by(bar) limit -1234`)
	f(`foo | sort by(bar) limit 12.34`)
	f(`foo | sort by(bar) limit 10 limit 20`)
	f(`foo | sort by(bar) offset`)
	f(`foo | sort by(bar) offset limit`)
	f(`foo | sort by(bar) offset -1234`)
	f(`foo | sort by(bar) offset 12.34`)
	f(`foo | sort by(bar) offset 10 offset 20`)

	// invalid uniq pipe
	f(`foo | uniq bar`)
	f(`foo | uniq limit`)
	f(`foo | uniq by(`)
	f(`foo | uniq by(a`)
	f(`foo | uniq by(a,`)
	f(`foo | uniq by(a) bar`)
	f(`foo | uniq by(a) limit -10`)
	f(`foo | uniq by(a) limit foo`)
}

func TestQueryGetNeededColumns(t *testing.T) {
	f := func(s, neededColumnsExpected, unneededColumnsExpected string) {
		t.Helper()

		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("cannot parse query %s: %s", s, err)
		}

		needed, unneeded := q.getNeededColumns()
		neededColumns := strings.Join(needed, ",")
		unneededColumns := strings.Join(unneeded, ",")

		if neededColumns != neededColumnsExpected {
			t.Fatalf("unexpected neededColumns; got %q; want %q", neededColumns, neededColumnsExpected)
		}
		if unneededColumns != unneededColumnsExpected {
			t.Fatalf("unexpected unneededColumns; got %q; want %q", unneededColumns, unneededColumnsExpected)
		}
	}

	f(`*`, `*`, ``)
	f(`foo bar`, `*`, ``)
	f(`foo:bar _time:5m baz`, `*`, ``)

	f(`* | fields *`, `*`, ``)
	f(`* | fields * | offset 10`, `*`, ``)
	f(`* | fields * | offset 10 | limit 20`, `*`, ``)
	f(`* | fields foo`, `foo`, ``)
	f(`* | fields foo, bar`, `bar,foo`, ``)
	f(`* | fields foo, bar | fields baz, bar`, `bar`, ``)
	f(`* | fields foo, bar | fields baz, a`, ``, ``)
	f(`* | fields f1, f2 | rm f3, f4`, `f1,f2`, ``)
	f(`* | fields f1, f2 | rm f2, f3`, `f1`, ``)
	f(`* | fields f1, f2 | rm f1, f2, f3`, ``, ``)
	f(`* | fields f1, f2 | cp f1 f2, f3 f4`, `f1`, ``)
	f(`* | fields f1, f2 | cp f1 f3, f4 f5`, `f1,f2`, ``)
	f(`* | fields f1, f2 | cp f2 f3, f4 f5`, `f1,f2`, ``)
	f(`* | fields f1, f2 | cp f2 f3, f4 f1`, `f2`, ``)
	f(`* | fields f1, f2 | mv f1 f2, f3 f4`, `f1`, ``)
	f(`* | fields f1, f2 | mv f1 f3, f4 f5`, `f1,f2`, ``)
	f(`* | fields f1, f2 | mv f2 f3, f4 f5`, `f1,f2`, ``)
	f(`* | fields f1, f2 | mv f2 f3, f4 f1`, `f2`, ``)
	f(`* | fields f1, f2 | stats count() r1`, ``, ``)
	f(`* | fields f1, f2 | stats count_uniq() r1`, `f1,f2`, ``)
	f(`* | fields f1, f2 | stats count(f1) r1`, `f1`, ``)
	f(`* | fields f1, f2 | stats count(f1,f2,f3) r1`, `f1,f2`, ``)
	f(`* | fields f1, f2 | stats by(b1) count() r1`, ``, ``)
	f(`* | fields f1, f2 | stats by(b1,f1) count() r1`, `f1`, ``)
	f(`* | fields f1, f2 | stats by(b1,f1) count(f1) r1`, `f1`, ``)
	f(`* | fields f1, f2 | stats by(b1,f1) count(f1,f2,f3) r1`, `f1,f2`, ``)
	f(`* | fields f1, f2 | sort by(f3)`, `f1,f2`, ``)
	f(`* | fields f1, f2 | sort by(f1,f3)`, `f1,f2`, ``)
	f(`* | fields f1, f2 | sort by(f3) | stats count() r1`, ``, ``)
	f(`* | fields f1, f2 | sort by(f1) | stats count() r1`, `f1`, ``)
	f(`* | fields f1, f2 | sort by(f1) | stats count(f2,f3) r1`, `f1,f2`, ``)
	f(`* | fields f1, f2 | sort by(f3) | fields f2`, `f2`, ``)
	f(`* | fields f1, f2 | sort by(f1,f3) | fields f2`, `f1,f2`, ``)

	f(`* | cp foo bar`, `*`, `bar`)
	f(`* | cp foo bar, baz a`, `*`, `a,bar`)
	f(`* | cp foo bar, baz a | fields foo,a,b`, `b,baz,foo`, ``)
	f(`* | cp foo bar, baz a | fields bar,a,b`, `b,baz,foo`, ``)
	f(`* | cp foo bar, baz a | fields baz,a,b`, `b,baz`, ``)
	f(`* | cp foo bar | fields bar,a`, `a,foo`, ``)
	f(`* | cp foo bar | fields baz,a`, `a,baz`, ``)
	f(`* | cp foo bar | fields foo,a`, `a,foo`, ``)
	f(`* | cp f1 f2 | rm f1`, `*`, `f2`)
	f(`* | cp f1 f2 | rm f2`, `*`, `f2`)
	f(`* | cp f1 f2 | rm f3`, `*`, `f2,f3`)

	f(`* | mv foo bar`, `*`, `bar`)
	f(`* | mv foo bar, baz a`, `*`, `a,bar`)
	f(`* | mv foo bar, baz a | fields foo,a,b`, `b,baz`, ``)
	f(`* | mv foo bar, baz a | fields bar,a,b`, `b,baz,foo`, ``)
	f(`* | mv foo bar, baz a | fields baz,a,b`, `b,baz`, ``)
	f(`* | mv foo bar, baz a | fields baz,foo,b`, `b`, ``)
	f(`* | mv foo bar | fields bar,a`, `a,foo`, ``)
	f(`* | mv foo bar | fields baz,a`, `a,baz`, ``)
	f(`* | mv foo bar | fields foo,a`, `a`, ``)
	f(`* | mv f1 f2 | rm f1`, `*`, `f2`)
	f(`* | mv f1 f2 | rm f2,f3`, `*`, `f1,f2,f3`)
	f(`* | mv f1 f2 | rm f3`, `*`, `f2,f3`)

	f(`* | sort by (f1)`, `*`, ``)
	f(`* | sort by (f1) | fields f2`, `f1,f2`, ``)
	f(`_time:5m | sort by (_time) | fields foo`, `_time,foo`, ``)
	f(`* | sort by (f1) | fields *`, `*`, ``)
	f(`* | sort by (f1) | sort by (f2,f3 desc) desc`, `*`, ``)
	f(`* | sort by (f1) | sort by (f2,f3 desc) desc | fields f4`, `f1,f2,f3,f4`, ``)
	f(`* | sort by (f1) | sort by (f2,f3 desc) desc | fields f4 | rm f1,f2,f5`, `f1,f2,f3,f4`, ``)

	f(`* | stats by(f1) count(f2) r1, count(f3,f4) r2`, `f1,f2,f3,f4`, ``)
	f(`* | stats by(f1) count(f2) r1, count(f3,f4) r2 | fields f5,f6`, ``, ``)
	f(`* | stats by(f1) count(f2) r1, count(f3,f4) r2 | fields f1,f5`, `f1`, ``)
	f(`* | stats by(f1) count(f2) r1, count(f3,f4) r2 | fields r1`, `f1,f2`, ``)
	f(`* | stats by(f1) count(f2) r1, count(f3,f4) r2 | fields r2,r3`, `f1,f3,f4`, ``)
	f(`_time:5m | stats by(_time:day) count() r1 | stats values(_time) r2`, `_time`, ``)
	f(`* | stats count(f1) r1 | stats count() r1`, ``, ``)
	f(`* | stats count(f1) r1 | stats count() r2`, ``, ``)
	f(`* | stats count(f1) r1 | stats count(r1) r2`, `f1`, ``)
	f(`* | stats count(f1) r1 | stats count(f1) r2`, ``, ``)
	f(`* | stats count(f1) r1 | stats count(f1,r1) r1`, `f1`, ``)
	f(`* | stats count(f1,f2) r1 | stats count(f2) r1, count(r1) r2`, `f1,f2`, ``)
	f(`* | stats count(f1,f2) r1 | stats count(f2) r1, count(r1) r2 | fields r1`, ``, ``)
	f(`* | stats count(f1,f2) r1 | stats count(f2) r1, count(r1) r2 | fields r2`, `f1,f2`, ``)
	f(`* | stats by(f3,f4) count(f1,f2) r1 | stats count(f2) r1, count(r1) r2 | fields r2`, `f1,f2,f3,f4`, ``)
	f(`* | stats by(f3,f4) count(f1,f2) r1 | stats count(f3) r1, count(r1) r2 | fields r1`, `f3,f4`, ``)

	f(`* | uniq`, `*`, ``)
	f(`* | uniq by (f1,f2)`, `f1,f2`, ``)
	f(`* | uniq by (f1,f2) | fields f1,f3`, `f1,f2`, ``)
	f(`* | uniq by (f1,f2) | rm f1,f3`, `f1,f2`, ``)
	f(`* | uniq by (f1,f2) | fields f3`, `f1,f2`, ``)

	f(`* | rm f1, f2`, `*`, `f1,f2`)
	f(`* | rm f1, f2 | mv f2 f3`, `*`, `f1,f2,f3`)
	f(`* | rm f1, f2 | cp f2 f3`, `*`, `f1,f2,f3`)
	f(`* | rm f1, f2 | mv f2 f3 | sort by(f4)`, `*`, `f1,f2,f3`)
	f(`* | rm f1, f2 | mv f2 f3 | sort by(f1)`, `*`, `f1,f2,f3`)
	f(`* | rm f1, f2 | fields f3`, `f3`, ``)
	f(`* | rm f1, f2 | fields f1,f3`, `f3`, ``)
	f(`* | rm f1, f2 | stats count() f1`, ``, ``)
	f(`* | rm f1, f2 | stats count(f3) r1`, `f3`, ``)
	f(`* | rm f1, f2 | stats count(f1) r1`, ``, ``)
	f(`* | rm f1, f2 | stats count(f1,f3) r1`, `f3`, ``)
	f(`* | rm f1, f2 | stats by(f1) count(f2) r1`, ``, ``)
	f(`* | rm f1, f2 | stats by(f3) count(f2) r1`, `f3`, ``)
	f(`* | rm f1, f2 | stats by(f3) count(f4) r1`, `f3,f4`, ``)
}
