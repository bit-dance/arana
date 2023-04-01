package dal

import (
	"context"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/plan/dal"
)

func init() {
	optimize.Register(ast.SQLTypeShowTableRules, optimizeShowTableRules)
}

func optimizeShowTableRules(_ context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	rule := o.Rule
	stmt := o.Stmt.(*ast.ShowTableRules)
	ret := dal.NewShowTableRules(stmt)
	ret.BindArgs(o.Args)
	ret.SetRule(rule)
	return ret, nil
}
