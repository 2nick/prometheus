package sharding

import (
	"testing"
	"github.com/prometheus/common/model"
)

func TestParseInvalid(t *testing.T) {
	query := "go{__name__=~\"test.+\"}"
	expectedError := "parse error at char 22: metric name must not be set twice: \"go\" or \"test.+\""

	_, err := Parse(query)
	if err == nil {
		t.Fatal("Must produce error")
	}

	if err.Error() != expectedError {
		t.Fatal("Got unexpected error: " + err.Error())
	}
}

type fixtureLabel struct {
	cmp   string
	value string
}

func TestParseValid(t *testing.T) {
	query := "{__name__=~\"test.+\",instance=\"antihost\"}"

	expectedMetricMatchersCount := 1

	labels := make(map[model.LabelName]fixtureLabel)

	labels["__name__"] = fixtureLabel{
		cmp:   "=~",
		value: "test.+",
	}

	labels["instance"] = fixtureLabel{
		cmp:   "=",
		value: "antihost",
	}

	metricMatchers, err := Parse(query)
	if err != nil {
		t.Fatalf("Unexpected parse error: %s", err.Error())
	}

	actualMetricMatchersCount := len(metricMatchers)
	if actualMetricMatchersCount != expectedMetricMatchersCount {
		t.Fatalf(
			"Unexpected metricMatchers count %d instead of %d",
			actualMetricMatchersCount,
			expectedMetricMatchersCount,
		)
	}

	for _, labelMatcher := range metricMatchers[0] {
		expectedLabel, ok := labels[labelMatcher.Name]
		if !ok {
			t.Fatalf("Got unexpected label: %+v", *labelMatcher)
		}

		if labelMatcher.Type.String() != expectedLabel.cmp {
			t.Fatalf("Got unexpected match type: %s", labelMatcher.Type.String())
		}

		if string(labelMatcher.Value) != expectedLabel.value {
			t.Fatalf("Got unexpected expr: %s", labelMatcher.Value)
		}
	}
}
