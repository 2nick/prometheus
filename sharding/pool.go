package sharding

import (
	"sync"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
)

var baseLogger = log.Base()

type PromServer interface {
}

type PromHTTPServer struct {
	PromServer
	Url string
}

type LabelsMap map[model.LabelName][]model.LabelValue

func (lm LabelsMap) addValue(key, value string) {
	ln := model.LabelName(key)
	lm[ln] = append(lm[ln], model.LabelValue(value))
}

type PromMockServer struct {
	labelsWithValuesMap LabelsMap
	*PromHTTPServer
}

func NewMockServer(url string, labelsMap *LabelsMap) *PromMockServer {
	var lm LabelsMap
	if labelsMap == nil {
		lm = make(LabelsMap)
	} else {
		lm = *labelsMap
	}

	return &PromMockServer{
		labelsWithValuesMap: lm,
		PromHTTPServer: &PromHTTPServer{
			Url: url,
		},
	}
}

func (s *PromMockServer) findLabels(queryTSMatchers []metric.LabelMatchers) *PromQueryServerMapping {
	serverLogger := baseLogger.With("server", s.Url)

	queryShardMapping := &PromQueryServerMapping{Server: s}
	queryShardMapping.Result = []MatchersMappingResult{}

	for _, matchersGroup := range queryTSMatchers {
		result := MatchersMappingResult{
			Matchers:  matchersGroup,
			LabelsMap: make(LabelsMap, len(queryTSMatchers)),
		}

		lmsLogger := serverLogger.With("matchers", matchersGroup)
		lmsLogger.Debug("-> Searching")
		found := "I have enough labels"
		for _, lm := range matchersGroup {
			values, ok := s.labelsWithValuesMap[lm.Name]
			if !ok {
				values = model.LabelValues{}
			}

			result.LabelsMap[lm.Name] = lm.Filter(values)

			if len(result.LabelsMap[lm.Name]) < 1 {
				found = ":("
			}

			lmsLogger.With("lm", lm).With("found", len(result.LabelsMap[lm.Name])).With("result", result.LabelsMap[lm.Name])
		}

		lmsLogger.Debug("<- ", found)

		queryShardMapping.Result = append(queryShardMapping.Result, result)
	}

	return queryShardMapping
}

type MatchersMappingResult struct {
	Matchers  metric.LabelMatchers
	LabelsMap LabelsMap
}

type PromQueryServerMapping struct {
	Server *PromMockServer
	Result []MatchersMappingResult
}

type PromPool interface {
	Plan(query string) ([]PromQueryServerMapping, error)
}

type promPool []*PromMockServer

func NewPromPool(servers []*PromMockServer) PromPool {
	pool := promPool(servers)
	return &pool
}

func (pp *promPool) Plan(query string) ([]PromQueryServerMapping, error) {
	queryTSMatchers, err := Parse(query)
	if err != nil {
		return nil, err
	}

	servers := *pp
	poolLength := len(servers)
	mappings := []PromQueryServerMapping{}
	mappingsChan := make(chan *PromQueryServerMapping, poolLength)

	var wg sync.WaitGroup

	wg.Add(poolLength)

	for _, s := range servers {
		//go func(ps PromMockServer, lms []metric.LabelMatchers) {
		mappingsChan <- s.findLabels(queryTSMatchers)
		wg.Done()
		//}(s, queryTSMatchers)
	}

	go func() {
		wg.Wait()
		close(mappingsChan)
	}()

	for mapping := range mappingsChan {
		mappings = append(mappings, *mapping)
	}

	return mappings, nil
}
