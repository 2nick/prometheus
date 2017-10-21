// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"golang.org/x/net/context"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/util/httputil"
)

type status string

const (
	statusSuccess status = "success"
	statusError          = "error"
)

type errorType string

const (
	errorNone     errorType = ""
	errorTimeout            = "timeout"
	errorCanceled           = "canceled"
	errorExec               = "execution"
	errorBadData            = "bad_data"
	errorInternal           = "internal"
)

var corsHeaders = map[string]string{
	"Access-Control-Allow-Headers":  "Accept, Authorization, Content-Type, Origin",
	"Access-Control-Allow-Methods":  "GET, OPTIONS",
	"Access-Control-Allow-Origin":   "*",
	"Access-Control-Expose-Headers": "Date",
}

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type targetRetriever interface {
	Targets() []*retrieval.Target
}

type alertmanagerRetriever interface {
	Alertmanagers() []*url.URL
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// Enables cross-site script calls.
func setCORS(w http.ResponseWriter) {
	for h, v := range corsHeaders {
		w.Header().Set(h, v)
	}
}

type apiFunc func(r *http.Request) (interface{}, *apiError)

// API can register a set of endpoints in a router and handle
// them using the provided storage and query engine.
type API struct {
	Storage     local.Storage
	QueryEngine *promql.Engine

	targetRetriever       targetRetriever
	alertmanagerRetriever alertmanagerRetriever

	now    func() model.Time
	config func() config.Config
	ready  func(http.HandlerFunc) http.HandlerFunc
}

// NewAPI returns an initialized API type.
func NewAPI(
	qe *promql.Engine,
	st local.Storage,
	tr targetRetriever,
	ar alertmanagerRetriever,
	configFunc func() config.Config,
	readyFunc func(http.HandlerFunc) http.HandlerFunc,
) *API {
	return &API{
		QueryEngine:           qe,
		Storage:               st,
		targetRetriever:       tr,
		alertmanagerRetriever: ar,
		now:    model.Now,
		config: configFunc,
		ready:  readyFunc,
	}
}

// Register the API's endpoints in the given router.
func (api *API) Register(r *route.Router) {
	instr := func(name string, f apiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			setCORS(w)
			if data, err := f(r); err != nil {
				respondError(w, err, data)
			} else if data != nil {
				respond(w, data)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})
		return api.ready(prometheus.InstrumentHandler(name, httputil.CompressionHandler{
			Handler: hf,
		}))
	}

	r.Options("/*path", instr("options", api.options))

	r.Get("/query", instr("query", api.query))
	r.Get("/query_range", instr("query_range", api.queryRange))

	r.Get("/label/:name/values", instr("label_values", api.labelValues))
	r.Get("/labels", instr("labels", api.labelNames))

	r.Get("/series", instr("series", api.series))
	r.Del("/series", instr("drop_series", api.dropSeries))

	r.Get("/targets", instr("targets", api.targets))
	r.Get("/alertmanagers", instr("alertmanagers", api.alertmanagers))

	r.Get("/status/config", instr("config", api.serveConfig))
	r.Post("/read", api.ready(prometheus.InstrumentHandler("read", http.HandlerFunc(api.remoteRead))))
}

type queryData struct {
	ResultType model.ValueType `json:"resultType"`
	Result     model.Value     `json:"result"`
}

func (api *API) options(r *http.Request) (interface{}, *apiError) {
	return nil, nil
}

func (api *API) query(r *http.Request) (interface{}, *apiError) {
	var ts model.Time
	if t := r.FormValue("time"); t != "" {
		var err error
		ts, err = parseTime(t)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}
	} else {
		ts = api.now()
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := api.QueryEngine.NewInstantQuery(r.FormValue("query"), ts)
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, &apiError{errorCanceled, res.Err}
		case promql.ErrQueryTimeout:
			return nil, &apiError{errorTimeout, res.Err}
		case promql.ErrStorage:
			return nil, &apiError{errorInternal, res.Err}
		}
		return nil, &apiError{errorExec, res.Err}
	}
	return &queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, nil
}

func (api *API) queryRange(r *http.Request) (interface{}, *apiError) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, &apiError{errorBadData, err}
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}

	if step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
		return nil, &apiError{errorBadData, err}
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return nil, &apiError{errorBadData, err}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := api.QueryEngine.NewRangeQuery(r.FormValue("query"), start, end, step)
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, &apiError{errorCanceled, res.Err}
		case promql.ErrQueryTimeout:
			return nil, &apiError{errorTimeout, res.Err}
		}
		return nil, &apiError{errorExec, res.Err}
	}
	return &queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, nil
}

func (api *API) labelNames(r *http.Request) (interface{}, *apiError) {
	q, err := api.Storage.Querier()
	if err != nil {
		return nil, &apiError{errorExec, err}
	}
	defer q.Close()

	vals, err := q.LabelNames(r.Context())
	if err != nil {
		return nil, &apiError{errorExec, err}
	}
	sort.Sort(vals)

	return vals, nil
}

func (api *API) labelValues(r *http.Request) (interface{}, *apiError) {
	name := route.Param(r.Context(), "name")

	if !model.LabelNameRE.MatchString(name) {
		return nil, &apiError{errorBadData, fmt.Errorf("invalid label name: %q", name)}
	}
	q, err := api.Storage.Querier()
	if err != nil {
		return nil, &apiError{errorExec, err}
	}
	defer q.Close()

	vals, err := q.LabelValuesForLabelName(r.Context(), model.LabelName(name))
	if err != nil {
		return nil, &apiError{errorExec, err}
	}
	sort.Sort(vals)

	return vals, nil
}

func (api *API) series(r *http.Request) (interface{}, *apiError) {
	r.ParseForm()
	if len(r.Form["match[]"]) == 0 {
		return nil, &apiError{errorBadData, fmt.Errorf("no match[] parameter provided")}
	}

	var start model.Time
	if t := r.FormValue("start"); t != "" {
		var err error
		start, err = parseTime(t)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}
	} else {
		start = model.Earliest
	}

	var end model.Time
	if t := r.FormValue("end"); t != "" {
		var err error
		end, err = parseTime(t)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}
	} else {
		end = model.Latest
	}

	var matcherSets []metric.LabelMatchers
	for _, s := range r.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}
		matcherSets = append(matcherSets, matchers)
	}

	q, err := api.Storage.Querier()
	if err != nil {
		return nil, &apiError{errorExec, err}
	}
	defer q.Close()

	res, err := q.MetricsForLabelMatchers(r.Context(), start, end, matcherSets...)
	if err != nil {
		return nil, &apiError{errorExec, err}
	}

	metrics := make([]model.Metric, 0, len(res))
	for _, met := range res {
		metrics = append(metrics, met.Metric)
	}
	return metrics, nil
}

func (api *API) dropSeries(r *http.Request) (interface{}, *apiError) {
	r.ParseForm()
	if len(r.Form["match[]"]) == 0 {
		return nil, &apiError{errorBadData, fmt.Errorf("no match[] parameter provided")}
	}

	numDeleted := 0
	for _, s := range r.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}
		n, err := api.Storage.DropMetricsForLabelMatchers(context.TODO(), matchers...)
		if err != nil {
			return nil, &apiError{errorExec, err}
		}
		numDeleted += n
	}

	res := struct {
		NumDeleted int `json:"numDeleted"`
	}{
		NumDeleted: numDeleted,
	}
	return res, nil
}

// Target has the information for one target.
type Target struct {
	// Labels before any processing.
	DiscoveredLabels model.LabelSet `json:"discoveredLabels"`
	// Any labels that are added to this target and its metrics.
	Labels model.LabelSet `json:"labels"`

	ScrapeURL string `json:"scrapeUrl"`

	LastError  string                 `json:"lastError"`
	LastScrape time.Time              `json:"lastScrape"`
	Health     retrieval.TargetHealth `json:"health"`
}

// TargetDiscovery has all the active targets.
type TargetDiscovery struct {
	ActiveTargets []*Target `json:"activeTargets"`
}

func (api *API) targets(r *http.Request) (interface{}, *apiError) {
	targets := api.targetRetriever.Targets()
	res := &TargetDiscovery{ActiveTargets: make([]*Target, len(targets))}

	for i, t := range targets {
		lastErrStr := ""
		lastErr := t.LastError()
		if lastErr != nil {
			lastErrStr = lastErr.Error()
		}

		res.ActiveTargets[i] = &Target{
			DiscoveredLabels: t.DiscoveredLabels(),
			Labels:           t.Labels(),
			ScrapeURL:        t.URL().String(),
			LastError:        lastErrStr,
			LastScrape:       t.LastScrape(),
			Health:           t.Health(),
		}
	}

	return res, nil
}

// AlertmanagerDiscovery has all the active Alertmanagers.
type AlertmanagerDiscovery struct {
	ActiveAlertmanagers []*AlertmanagerTarget `json:"activeAlertmanagers"`
}

// AlertmanagerTarget has info on one AM.
type AlertmanagerTarget struct {
	URL string `json:"url"`
}

func (api *API) alertmanagers(r *http.Request) (interface{}, *apiError) {
	urls := api.alertmanagerRetriever.Alertmanagers()
	ams := &AlertmanagerDiscovery{ActiveAlertmanagers: make([]*AlertmanagerTarget, len(urls))}

	for i, url := range urls {
		ams.ActiveAlertmanagers[i] = &AlertmanagerTarget{URL: url.String()}
	}

	return ams, nil
}

type prometheusConfig struct {
	YAML string `json:"yaml"`
}

func (api *API) serveConfig(r *http.Request) (interface{}, *apiError) {
	cfg := &prometheusConfig{
		YAML: api.config().String(),
	}
	return cfg, nil
}

func (api *API) remoteRead(w http.ResponseWriter, r *http.Request) {
	req, err := remote.DecodeReadRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := remote.ReadResponse{
		Results: make([]*remote.QueryResult, len(req.Queries)),
	}
	for i, query := range req.Queries {
		querier, err := api.Storage.Querier()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer querier.Close()

		from, through, matchers, err := remote.FromQuery(query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		iters, err := querier.QueryRange(r.Context(), from, through, matchers...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp.Results[i] = remote.ToQueryResult(remote.IteratorsToMatrix(iters, metric.Interval{
			OldestInclusive: from,
			NewestInclusive: through,
		}))
		externalLabels := api.config().GlobalConfig.ExternalLabels.Clone()
		for _, ts := range resp.Results[i].Timeseries {
			globalUsed := map[string]struct{}{}
			for _, l := range ts.Labels {
				if _, ok := externalLabels[model.LabelName(l.Name)]; ok {
					globalUsed[l.Name] = struct{}{}
				}
			}
			for ln, lv := range externalLabels {
				if _, ok := globalUsed[string(ln)]; !ok {
					ts.Labels = append(ts.Labels, &remote.LabelPair{
						Name:  string(ln),
						Value: string(lv),
					})
				}
			}
		}
	}

	if err := remote.EncodeReadResponse(&resp, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func respond(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	b, err := json.Marshal(&response{
		Status: statusSuccess,
		Data:   data,
	})
	if err != nil {
		return
	}
	w.Write(b)
}

func respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	default:
		code = http.StatusInternalServerError
	}
	w.WriteHeader(code)

	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		return
	}
	w.Write(b)
}

func parseTime(s string) (model.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		ts := t * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid timestamp. It overflows int64", s)
		}
		return model.TimeFromUnixNano(int64(ts)), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return model.TimeFromUnixNano(t.UnixNano()), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
