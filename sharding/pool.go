package sharding

import (
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
)

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

func NewMockServer(url string, labelsMap *LabelsMap) PromMockServer {
	var lm LabelsMap
	if labelsMap == nil {
		lm = make(LabelsMap)
	} else {
		lm = *labelsMap
	}

	return PromMockServer{
		labelsWithValuesMap: lm,
		PromHTTPServer: &PromHTTPServer{
			Url: url,
		},
	}
}

func (s *PromMockServer) findLabels(lms metric.LabelMatchers) PromQueryShardMapping {
	labels := make(LabelsMap, len(lms))

	for _, lm := range lms {
		values, ok := s.labelsWithValuesMap[lm.Name]
		if !ok {
			values = model.LabelValues{}
		}

		labels[lm.Name] = lm.Filter(values)
	}

	return PromQueryShardMapping{
		Server:    s,
		Matchers:  lms,
		LabelsMap: labels,
	}
}

type PromQueryShardMapping struct {
	Server    *PromMockServer
	Matchers  metric.LabelMatchers
	LabelsMap LabelsMap
}

type PromPool interface {
	Plan([]metric.LabelMatchers) ([]PromQueryShardMapping, error)
}

type promPool []PromMockServer

func NewPromPool(servers []PromMockServer) PromPool {
	return promPool(servers)
}

func (pp promPool) Plan(matchers []metric.LabelMatchers) ([]PromQueryShardMapping, error) {
	mappingsCount := len(matchers) * len(pp)

	mappings := []PromQueryShardMapping{}
	mappingsChan := make(chan PromQueryShardMapping, mappingsCount)

	var wg sync.WaitGroup

	wg.Add(mappingsCount)

	for _, s := range pp {
		for _, labelsMatchers := range matchers {
			go func(ps PromMockServer, lms metric.LabelMatchers) {
				mappingsChan <- ps.findLabels(lms)
				wg.Done()
			}(s, labelsMatchers)
		}
	}

	go func() {
		wg.Wait()
		close(mappingsChan)
	}()

	for mapping := range mappingsChan {
		mappings = append(mappings, mapping)
	}

	return mappings, nil
}
