package memebridge

import (
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/samber/lo"
)

type sign = int64

const (
	signPositive = 1
	signNegative = -1
)

func parseSign(s string) sign {
	switch s {
	case "-":
		return signNegative
	default:
		return signPositive
	}
}

func formatNumericForInterval(r *big.Rat) string {
	if r.IsInt() {
		return r.RatString()
	}

	// Strip trailing zero of nano-precision decimal
	return strings.TrimRight(spanner.NumericString(r), "0")
}

type dateTimePart string

const (
	dateTimePartYear   dateTimePart = "YEAR"
	dateTimePartMonth  dateTimePart = "MONTH"
	dateTimePartDay    dateTimePart = "DAY"
	dateTimePartHour   dateTimePart = "HOUR"
	dateTimePartMinute dateTimePart = "MINUTE"
	dateTimePartSecond dateTimePart = "SECOND"

	// QUARTER, WEEK, MILLISECOND, MICROSECOND, NANOSECOND are valid only in IntervalLiteralSingle

	dateTimePartQuarter     dateTimePart = "QUARTER"
	dateTimePartWeek        dateTimePart = "WEEK"
	dateTimePartMillisecond dateTimePart = "MILLISECOND"
	dateTimePartMicrosecond dateTimePart = "MICROSECOND"
	dateTimePartNanosecond  dateTimePart = "NANOSECOND"
)

func parseDateTimePart(s string) (dateTimePart, error) {
	switch part := dateTimePart(strings.ToUpper(s)); part {
	case dateTimePartYear, dateTimePartMonth, dateTimePartDay, dateTimePartHour, dateTimePartMinute, dateTimePartSecond,
		dateTimePartQuarter, dateTimePartWeek, dateTimePartMillisecond, dateTimePartMicrosecond, dateTimePartNanosecond:
		return part, nil
	default:
		return "", fmt.Errorf("unknown datetime part: %v", s)
	}
}

func toRFC8601Duration(i int64, part dateTimePart) (string, error) {
	if i == 0 {
		return "P0Y", nil
	}

	// https://cloud.google.com/spanner/docs/reference/standard-sql/data-types#interval_datetime_parts
	switch part {
	case dateTimePartYear:
		return fmt.Sprintf("P%vY", i), nil
	case dateTimePartQuarter:
		return fmt.Sprintf("P%vM", 3*i), nil
	case dateTimePartMonth:
		return fmt.Sprintf("P%vM", i), nil
	case dateTimePartWeek:
		return fmt.Sprintf("P%vD", 7*i), nil
	case dateTimePartDay:
		return fmt.Sprintf("P%vD", i), nil
	case dateTimePartHour:
		return fmt.Sprintf("PT%vH", i), nil
	case dateTimePartMinute:
		return fmt.Sprintf("PT%vM", i), nil
	case dateTimePartSecond:
		return fmt.Sprintf("PT%vS", i), nil
	case dateTimePartMillisecond:
		return fmt.Sprintf("PT%vS", formatNumericForInterval(big.NewRat(i, 1000))), nil
	case dateTimePartMicrosecond:
		return fmt.Sprintf("PT%vS", formatNumericForInterval(big.NewRat(i, 1000*1000))), nil
	case dateTimePartNanosecond:
		return fmt.Sprintf("PT%vS", formatNumericForInterval(big.NewRat(i, 1000*1000*1000))), nil
	default:
		return "", fmt.Errorf("unknown datetime part: %v", part)
	}
}

func mustCompileDateTimeRe(datePart, timePart string) *regexp.Regexp {
	return regexp.MustCompile(`^` + datePart +
		lo.Ternary(datePart != "" && timePart != "", " ", "") +
		timePart + `$`)
}

var (
	// Date part
	yearMonthSignReStr = `(?P<yearMonthSign>[-+]?)`
	yearReStr          = `(?P<year>\d+)`
	monthReStr         = `(?P<month>\d+)`
	dayReStr           = `(?P<daySign>[-+]?)(?P<day>\d+)`
	yearToDayRe        = yearMonthSignReStr + yearReStr + `-` + monthReStr + ` ` + dayReStr
	yearToMonthRe      = yearMonthSignReStr + yearReStr + `-` + monthReStr
	monthToDayRe       = yearMonthSignReStr + monthReStr + ` ` + dayReStr
	dayRe              = dayReStr

	// Time part
	timeSignReStr    = `(?P<timeSign>[-+]?)`
	hourReStr        = `(?P<hour>\d+)`
	minuteReStr      = `(?P<minute>\d+)`
	secondReStr      = `(?P<second>\d+(?:\.\d+)?)`
	hourRe           = timeSignReStr + hourReStr
	hourToMinuteRe   = timeSignReStr + hourReStr + `:` + minuteReStr
	hourToSecondRe   = timeSignReStr + hourReStr + `:` + minuteReStr + `:` + secondReStr
	minuteToSecondRe = timeSignReStr + minuteReStr + `:` + secondReStr

	dateTimeRangeRegexpMap = map[dateTimePart]map[dateTimePart]*regexp.Regexp{
		dateTimePartYear: {
			dateTimePartMonth:  mustCompileDateTimeRe(yearToMonthRe, ""),
			dateTimePartDay:    mustCompileDateTimeRe(yearToDayRe, ""),
			dateTimePartHour:   mustCompileDateTimeRe(yearToDayRe, hourRe),
			dateTimePartMinute: mustCompileDateTimeRe(yearToDayRe, hourToMinuteRe),
			dateTimePartSecond: mustCompileDateTimeRe(yearToDayRe, hourToSecondRe),
		},
		dateTimePartMonth: {
			dateTimePartDay:    mustCompileDateTimeRe(monthToDayRe, ""),
			dateTimePartHour:   mustCompileDateTimeRe(monthToDayRe, hourRe),
			dateTimePartMinute: mustCompileDateTimeRe(monthToDayRe, hourToMinuteRe),
			dateTimePartSecond: mustCompileDateTimeRe(monthToDayRe, hourToSecondRe),
		},
		dateTimePartDay: {
			dateTimePartHour:   mustCompileDateTimeRe(dayRe, hourRe),
			dateTimePartMinute: mustCompileDateTimeRe(dayRe, hourToMinuteRe),
			dateTimePartSecond: mustCompileDateTimeRe(dayRe, hourToSecondRe),
		},
		dateTimePartHour: {
			dateTimePartMinute: mustCompileDateTimeRe("", hourToMinuteRe),
			dateTimePartSecond: mustCompileDateTimeRe("", hourToSecondRe),
		},
		dateTimePartMinute: {
			dateTimePartSecond: mustCompileDateTimeRe("", minuteToSecondRe),
		},
	}
)

func astIntervalLiteralsToGCV(expr ast.Expr) (spanner.GenericColumnValue, error) {
	interval, err := astIntervalLiteralsToInterval(expr)
	if err != nil {
		return zeroGCV, err
	}

	return gcvctor.IntervalValue(interval), nil
}

func astIntervalLiteralsToInterval(expr ast.Expr) (spanner.Interval, error) {
	var zero spanner.Interval

	switch e := expr.(type) {
	case *ast.IntervalLiteralSingle:
		intLiteral, ok := e.Value.(*ast.IntLiteral)
		if !ok {
			return zero, fmt.Errorf("expect int literal, but %v", e.Value)
		}

		i, err := strconv.ParseInt(intLiteral.Value, intLiteral.Base, 64)
		if err != nil {
			return zero, err
		}

		part, err := parseDateTimePart(e.DateTimePart.Name)
		if err != nil {
			return zero, err
		}

		durationString, err := toRFC8601Duration(i, part)
		if err != nil {
			return zero, err
		}

		return spanner.ParseInterval(durationString)
	case *ast.IntervalLiteralRange:
		start, err := parseDateTimePart(e.StartingDateTimePart.Name)
		if err != nil {
			return zero, err
		}

		end, err := parseDateTimePart(e.EndingDateTimePart.Name)
		if err != nil {
			return zero, err
		}

		mapForStart, ok := dateTimeRangeRegexpMap[start]
		if !ok {
			return zero, fmt.Errorf("start datetime part is not compatible with interval literal range: %v", start)
		}

		re, ok := mapForStart[end]
		if !ok {
			return zero, fmt.Errorf("datetime range is not compatible with interval literal range: start=%v, end=%v", start, end)
		}

		if !re.MatchString(e.Value.Value) {
			return zero, fmt.Errorf("interval literal with a datetime part range is not valid: sql: %v, regexp: %v", e.Value.Value, re.String())
		}

		matches := re.FindStringSubmatch(e.Value.Value)

		var yearMonthSign, daySign, timeSign sign
		var year, month, day, hour, minute int64
		second := new(big.Rat)

		for i, name := range re.SubexpNames() {
			s := matches[i]

			switch name {
			case "yearMonthSign":
				yearMonthSign = parseSign(s)
			case "daySign":
				daySign = parseSign(s)
			case "timeSign":
				timeSign = parseSign(s)
			case "year":
				year, err = strconv.ParseInt(s, 10, 64)
			case "month":
				month, err = strconv.ParseInt(s, 10, 64)
			case "day":
				day, err = strconv.ParseInt(s, 10, 64)
			case "hour":
				hour, err = strconv.ParseInt(s, 10, 64)
			case "minute":
				minute, err = strconv.ParseInt(s, 10, 64)
			case "second":
				second, ok = second.SetString(s)
				if !ok {
					return zero, fmt.Errorf("invalid second: %v", s)
				}
			}

			if err != nil {
				return zero, err
			}

		}

		nanosRat := new(big.Rat).Mul(
			big.NewRat(timeSign*1_000_000_000, 1),
			new(big.Rat).Add(big.NewRat(hour*3600+minute*60, 1), second))
		if !nanosRat.IsInt() {
			return zero, fmt.Errorf("invalid non-integer nanoseconds: %v", nanosRat)
		}

		return spanner.Interval{
			Months: int32(yearMonthSign*12*year + month),
			Days:   int32(daySign * day),
			Nanos:  nanosRat.Num(),
		}, nil
	default:
		return zero, fmt.Errorf("expr is not interval literal: %v", e)
	}
}
