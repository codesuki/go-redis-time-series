package timeseries

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrBadRange indicates that the given range is invalid. Start should always be <= End
	ErrBadRange = errors.New("timeseries: range is invalid")
)

type Redis interface {
	Incr(key string) (int, error)
	ZAdd(key string, score float64, member string) error
	ZCount(key string, start interface{}, end interface{}) (int, error)
	Expire(key string, seconds int) error
}

// TODO: add clock parameter
type TimeSeries struct {
	redis Redis

	name       string
	seriesKey  string
	counterKey string

	timestep time.Duration
	ttl      time.Duration
}

func NewTimeSeries(name string, timestep time.Duration, ttl time.Duration, redis Redis) *TimeSeries {
	seriesKey := fmt.Sprintf("%s:ts", name)
	counterKey := fmt.Sprintf("%s:counter", name)
	return &TimeSeries{
		redis:      redis,
		name:       name,
		seriesKey:  seriesKey,
		counterKey: counterKey,
		timestep:   timestep,
		ttl:        ttl,
	}
}

// IncreaseAtTime adds amount at a specific time.
func (ts *TimeSeries) IncreaseAtTime(amount int, t time.Time) error {
	for i := 0; i < amount; i++ {
		counterKey := ts.makeKey(ts.counterKey, t)
		id, err := ts.redis.Incr(counterKey)
		ts.redis.Expire(counterKey, int(ts.ttl.Seconds()))
		if err != nil {
			return err
		}
		seriesKey := ts.makeKey(ts.seriesKey, t)
		err = ts.redis.ZAdd(
			seriesKey,
			float64(t.Unix()),
			fmt.Sprintf("%s:%d", ts.name, id),
		)
		if err != nil {
			return err
		}
		ts.redis.Expire(seriesKey, int(ts.ttl.Seconds()))
	}
	return nil
}

// Range returns the sum over the given range [start, end).
// ErrBadRange is returned if start is after end.
func (ts *TimeSeries) Range(start time.Time, end time.Time) (float64, error) {
	if start.After(end) {
		return 0, ErrBadRange
	}
	// TODO: handle end being exclusive with (end
	totalCount := 0
	current := start.Truncate(ts.timestep)
	for {
		seriesKey := ts.makeKey(ts.seriesKey, current)
		count, err := ts.redis.ZCount(seriesKey, start.Unix(), end.Unix())
		if err != nil {
			return 0.0, err
		}
		totalCount += count

		current = current.Add(ts.timestep)
		if current.After(end) {
			break
		}
	}
	return float64(totalCount), nil
}

func (ts *TimeSeries) makeKey(prefix string, t time.Time) string {
	return fmt.Sprintf("%s:%d", prefix, t.Truncate(ts.timestep).Unix())
}
