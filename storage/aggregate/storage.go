package aggregate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

type Storage struct {
	storage.Storage
	aggregator *aggregator
}

type agValues map[string]float64

type aggregator struct {
	sync.Mutex

	matchers []*labels.Matcher

	prevValues agValues
	currValues agValues
}

func newAggregator(matchers []*labels.Matcher) *aggregator {
	return &aggregator{
		matchers:   matchers,
		prevValues: make(agValues),
		currValues: make(agValues),
	}
}

func (ag *aggregator) tickValues() (agValues, agValues, time.Time) {
	ag.Lock()
	defer ag.Unlock()

	prevValues, currValues := ag.prevValues, ag.currValues

	ag.prevValues = ag.currValues
	ag.currValues = make(agValues)

	return prevValues, currValues, time.Now()
}

func (ag *aggregator) AddValue(k string, v float64) {
	ag.Lock()
	defer ag.Unlock()

	ag.currValues[k] = v
}

func NewAggregateStorage() (storage.Storage, error) {
	s := `{__name__=~"up"}`
	matchers, err := promql.ParseMetricSelector(s)
	if err != nil {
		return nil, err
	}

	fmt.Printf("agg :: %s\n", matchers)

	st := &Storage{aggregator: newAggregator(matchers)}

	go st.aggregate()

	return st, nil
}

func (s *Storage) aggregate() {
	t := time.NewTicker(time.Minute)
	for {
		select {
		case <-t.C:
			pv, cv, tt := s.aggregator.tickValues()
			var sum float64
			for cvk, cvv := range cv {
				sum = sum + cvv - pv[cvk]
			}
			fmt.Printf(
				"SUM(RATE(%s[1m])) @ %s = %f\n",
				s.aggregator.matchers,
				tt,
				sum,
			)
		}
	}
}

// StartTime returns the oldest timestamp stored in the storage.
func (s *Storage) StartTime() (int64, error) {
	return 0, nil
}

// Querier returns a new Querier on the storage.
func (s *Storage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return storage.NoopQuerier(), nil
}

// Appender returns a new appender against the storage.
func (s *Storage) Appender() (storage.Appender, error) {
	matchersMap := make(map[string][]*labels.Matcher)
	for _, m := range s.aggregator.matchers {
		if _, ok := matchersMap[m.Name]; !ok {
			matchersMap[m.Name] = []*labels.Matcher{}
		}

		matchersMap[m.Name] = append(matchersMap[m.Name], m)
	}

	return &nopAppender{
		matchers: matchersMap,
		s:        s,
	}, nil
}

// Close closes the storage and all its underlying resources.
func (s *Storage) Close() error {
	return nil
}

type nopAppender struct {
	matchers map[string][]*labels.Matcher
	s        *Storage
}

func (a nopAppender) Add(lbs labels.Labels, t int64, value float64) (uint64, error) {
	valid := true
Validation:
	for _, lb := range lbs {
		if ms, ok := a.matchers[lb.Name]; ok {
			for _, m := range ms {
				if valid = m.Matches(lb.Value); !valid {
					break Validation
				}
			}
		}
	}

	if valid {
		fmt.Printf("agg :: %s %d %f\n", lbs, t, value)
		a.s.aggregator.AddValue(lbs.String(), value)
	}

	return 0, nil
}
func (a nopAppender) AddFast(labels.Labels, uint64, int64, float64) error { return nil }
func (a nopAppender) Commit() error                                       { return nil }
func (a nopAppender) Rollback() error                                     { return nil }
