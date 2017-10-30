package sharding

import (
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage/metric"
)

type visitor struct {
	labelMatchers []metric.LabelMatchers
}

func (v *visitor) Visit(node promql.Node) promql.Visitor {
	switch node.(type) {
	case *promql.VectorSelector:
		v.grabLabelMatchers(node)
		return v
	case *promql.MatrixSelector:
		v.grabLabelMatchers(node)
		return v
		//default:
		//	fmt.Printf(">> %T %+v\n", node, node)
	}

	return v
}
// private function with `interface{}` arg needs here
// because *Selector-s doesn't implement promql.Node interface
// because they both hove no String() function
func (v *visitor) grabLabelMatchers(s interface{}) {
	if ss, ok := s.(*promql.MatrixSelector); ok {
		//fmt.Printf(">>> %T %+v\n", ss, *ss)
		v.labelMatchers = append(v.labelMatchers, ss.LabelMatchers)
	} else if ss, ok := s.(*promql.VectorSelector); ok {
		//fmt.Printf(">>> %T %+v\n", ss, *ss)
		v.labelMatchers = append(v.labelMatchers, ss.LabelMatchers)
	}
}

func Parse(query string) ([]metric.LabelMatchers, error) {
	expr, err := promql.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	v := &visitor{}
	promql.Walk(v, expr)

	return v.labelMatchers, nil
}
