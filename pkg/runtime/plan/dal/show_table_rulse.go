package dal

import (
	"context"
	"fmt"
	"github.com/arana-db/arana/pkg/mysql/thead"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
)

var (
	_ proto.Plan = (*ShowTableRules)(nil)
)

type ShowTableRules struct {
	plan.BasePlan
	Stmt *ast.ShowTableRules
	rule *rule.Rule
}

func NewShowTableRules(stmt *ast.ShowTableRules) *ShowTableRules {
	return &ShowTableRules{
		Stmt: stmt,
	}
}

func (st *ShowTableRules) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (st *ShowTableRules) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		err     error
		table   string
	)
	ctx, span := plan.Tracer.Start(ctx, "ShowTableRules.ExecIn")
	defer span.End()

	if err = st.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	table = sb.String()

	fields := thead.Rules.ToFields()

	ds := &dataset.VirtualDataset{
		Columns: fields,
	}
	vtable, ok := st.rule.VTable(table)
	if !ok {
		return nil, errors.New(fmt.Sprintf("%s do not have %s's rules", rcontext.Schema(ctx), table))
	}
	ds.Close()
	vtable.Name()
	return nil, err
}

func (st *ShowTableRules) SetRule(rule *rule.Rule) {
	st.rule = rule
}
