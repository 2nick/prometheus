// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/sharding"
	"github.com/prometheus/prometheus/storage/fanin"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/util/cli"

	"golang.org/x/net/context"
)

//var defaultQuery = "sum(rate(mmm{instance=~\"aero.+\"}[1h]))"
//var defaultQuery = "mmm{instance=~\"aero.+\"} + mmm{instance!=\"wave\"} * 123 + sum(rate({__name__=~\"go.*\"}[10h]))"
var defaultQuery = "irate(prometheus_local_storage_ingested_samples_total{job='prometheus'}[5m])"

var serversConfig = map[string]*sharding.LabelsMap{
	"server.a": &sharding.LabelsMap{
		"__name__": []model.LabelValue{
			"test",
			"go",
			"mmm",
		},
		"instance": []model.LabelValue{
			"aerohokey",
			"nonono",
		},
	},
	"server.b": &sharding.LabelsMap{
		"__name__": []model.LabelValue{
			"test",
			"net",
		},
		"instance": []model.LabelValue{
			"test",
		},
	},
	"server.c": &sharding.LabelsMap{
		"__name__": []model.LabelValue{
			"test",
			"net",
			"go",
		},
		"instance": []model.LabelValue{
			"test",
			"aerowater",
		},
	},
	"http://localhost:9090": &sharding.LabelsMap{
		"__name__": []model.LabelValue{
			"prometheus_local_storage_ingested_samples_total",
		},
		"job": []model.LabelValue{
			"prometheus",
		},
	},
	"http://localhost:9091": &sharding.LabelsMap{
		"__name__": []model.LabelValue{
			"prometheus_local_storage_ingested_samples_total",
		},
		"job": []model.LabelValue{
			"prometheus",
		},
	},
}

var servers = []*sharding.PromMockServer{}

func init() {
	for serverURL, labelsMap := range serversConfig {
		servers = append(
			servers,
			sharding.NewMockServer(serverURL, labelsMap),
		)
	}
}

// RunQueryCmd runs query
func RunQueryCmd(t cli.Term, args ...string) int {
	stmt, err := parseArgs(args)
	if err != nil {
		t.Errorf("Error:\n%s\n\n", err.Error())
		t.Infof("usage: promsharder run -q <query> -s <start-date> -e <end-date> -st <step> [-t <timeout>]")
		return 1
	}

	pool := sharding.NewPromPool(servers)
	mappings, err := pool.Plan(stmt.query)
	if err != nil {
		t.Errorf("Pool error:\n%s\n\n", err.Error())
		return 2
	}

	cfg, err := config.Load("")
	for i, mapping := range mappings {
		if i > 0 {
			fmt.Println()
		}

		foundAll := true
		for _, mappingResult := range mapping.Result {
			for _, lv := range mappingResult.LabelsMap {
				if len(lv) < 1 {
					foundAll = false
					break
				}
			}
		}

		if !foundAll {
			continue
		}

		serverURL, err := url.Parse(mapping.Server.Url + "/api/v1/read")
		if err != nil {
			t.Errorf("Error during parsing server url", err.Error())
			return 2
		}

		cfg.RemoteReadConfigs = append(
			cfg.RemoteReadConfigs,
			&config.RemoteReadConfig{
				URL:           &config.URL{serverURL},
				RemoteTimeout: model.Duration(1 * time.Second),
			},
		)
	}

	queryable := fanin.Queryable{
		Local:  &local.NoopStorage{},
		Remote: &remote.Reader{},
	}

	queryable.Remote.ApplyConfig(cfg)

	engine := promql.NewEngine(
		&queryable,
		nil,
	)

	query, err := engine.NewInstantQuery(stmt.query, model.Now())
	if err != nil {
		t.Errorf("Error during create query", err.Error())
		return 2
	}

	result := query.Exec(context.Background())

	fmt.Println(result.String())

	return 0
}

// ExplainQueryCmd explains query
func ExplainQueryCmd(t cli.Term, args ...string) int {
	stmt, err := parseArgs(args)
	if err != nil {
		t.Errorf("Error:\n%s\n\n", err.Error())
		t.Infof("usage: promsharder explain -q <query> -s <start-date> -e <end-date> -st <step> [-t <timeout>]")
		return 1
	}

	pool := sharding.NewPromPool(servers)
	mappings, err := pool.Plan(stmt.query)
	if err != nil {
		t.Errorf("Pool error:\n%s\n\n", err.Error())
		return 2
	}

	for i, mapping := range mappings {
		if i > 0 {
			fmt.Println()
		}

		lines := []string{}

		lines = append(lines, mapping.Server.Url)

		prefix := "++"
		for l, mappingResult := range mapping.Result {
			if l > 0 {
				lines = append(lines, "\n")
			}

			lines = append(lines, fmt.Sprintf("%+v", mappingResult.Matchers))
			for lm, lv := range mappingResult.LabelsMap {
				localPrefix := "++"
				if len(lv) < 1 {
					prefix = "--"
					localPrefix = "--"
				}

				lines = append(lines, fmt.Sprintf("%s %s = %+v", localPrefix, lm, lv))
			}
		}

		for _, line := range lines {
			line = strings.Trim(line, "\n")
			if len(line) > 0 {
				line = strings.Repeat(prefix, 2) + " " + line
			}

			fmt.Println(line)
		}
	}

	//v, err := queryRange(stmt)
	//if err != nil {
	//	t.Errorf("Error:\n%s\n\n", err.Error())
	//	return 3
	//}
	//
	//fmt.Printf("%T\n", v.Value)

	return 0
}

type statement struct {
	query   string
	start   model.Time
	end     model.Time
	step    time.Duration
	timeout string
}

func queryRange(stmt *statement) (*promql.Result, error) {
	if stmt.end.Before(stmt.start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, err
	}

	if stmt.step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
		return nil, err
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if stmt.end.Sub(stmt.start)/stmt.step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return nil, err
	}

	ctx := context.Background()
	if to := stmt.timeout; to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, err
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	remoteReader := &remote.Reader{}
	localStorage := &local.NoopStorage{}

	queryable := fanin.Queryable{
		Local:  localStorage,
		Remote: remoteReader,
	}

	queryEngine := promql.NewEngine(queryable, promql.DefaultEngineOptions)

	qry, err := queryEngine.NewRangeQuery(stmt.query, stmt.start, stmt.end, stmt.step)
	if err != nil {
		return nil, err
	}

	fmt.Printf("%+v\n", qry.Statement())

	return qry.Exec(ctx), nil
}

func parseArgs(args []string) (*statement, error) {
	cmdArgs := &struct {
		query   string
		start   string
		end     string
		step    string
		timeout string
	}{}

	fs := flag.CommandLine

	fs.StringVar(&cmdArgs.query, "q", defaultQuery, "query")
	fs.StringVar(&cmdArgs.start, "s", "2017-10-21T00:00:00+00:00", "start")
	fs.StringVar(&cmdArgs.end, "e", "2017-10-22T00:00:00+00:00", "end")
	fs.StringVar(&cmdArgs.step, "st", "30s", "step")
	fs.StringVar(&cmdArgs.timeout, "t", "", "timeout")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	start, err := parseTime(cmdArgs.start)
	if err != nil {
		return nil, err
	}
	end, err := parseTime(cmdArgs.end)
	if err != nil {
		return nil, err
	}

	step, err := parseDuration(cmdArgs.step)
	if err != nil {
		return nil, err
	}

	return &statement{
		query:   cmdArgs.query,
		start:   start,
		end:     end,
		step:    step,
		timeout: cmdArgs.timeout,
	}, nil
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

// VersionCmd prints the binaries version information.
func VersionCmd(t cli.Term, _ ...string) int {
	fmt.Fprintln(os.Stdout, version.Print("promsharder"))
	return 0
}

func main() {
	app := cli.NewApp("promsharder")

	app.Register("explain", &cli.Command{
		Desc: "explain query",
		Run:  ExplainQueryCmd,
	})

	app.Register("run", &cli.Command{
		Desc: "run query",
		Run:  RunQueryCmd,
	})

	app.Register("version", &cli.Command{
		Desc: "print the version of this binary",
		Run:  VersionCmd,
	})

	t := cli.BasicTerm(os.Stdout, os.Stderr)
	os.Exit(app.Run(t, os.Args[1:]...))
}
