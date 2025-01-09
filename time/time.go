package time

import (
	"time"
	_ "time/tzdata"
)

func LocationStartOfDayInUTC(timezone string) (time.Time, error) {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, err
	}

	now := time.Now().In(location)
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, location)

	return startOfDay.UTC(), nil
}

func LocationEndOfDayInUTC(timezone string) (time.Time, error) {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, err
	}

	now := time.Now().In(location)
	endOfDay := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, location)

	return endOfDay.UTC(), nil
}
