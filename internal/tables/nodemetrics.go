package tables

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"k8s.io/client-go/tools/cache"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

func StartNodeMetricsInformer(ctx context.Context, db *memory.Database) {
	startMetricsInformer(ctx, db, &v1beta1.NodeMetrics{}, "nodes", initNodeMetricsTable, onAddNodeMetrics, onUpdateNodeMetrics, onDelNodeMetrics)
}

type NodeMetricsTable struct {
	db     *memory.Database
	table  *memory.Table
	logger *logrus.Entry
}

func initNodeMetricsTable(db *memory.Database) {
	if _, ok := tables[NodeMetricsTableName]; !ok {
		tables[NodeMetricsTableName] = &NodeMetricsTable{
			db:     db,
			table:  createNodeMetricsTable(db),
			logger: tableLogger(NodeMetricsTableName),
		}
	}

}

func createNodeMetricsTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(NodeMetricsTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: NodeMetricsTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: NodeMetricsTableName},
		{Name: "window", Type: sql.Int64, Nullable: false, Source: NodeMetricsTableName},
		{Name: "usage_memory", Type: sql.Int64, Nullable: false, Source: NodeMetricsTableName},
		{Name: "usage_cpu", Type: sql.Int64, Nullable: false, Source: NodeMetricsTableName},
		{Name: "usage_disk", Type: sql.Int64, Nullable: false, Source: NodeMetricsTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: NodeMetricsTableName},
	}), db.GetForeignKeyCollection())

	db.AddTable(NodeMetricsTableName, table)
	log.Infof("table [%s] created", NodeMetricsTableName)
	return table
}

func (t *NodeMetricsTable) Log() *logrus.Entry {
	return t.logger
}

func (t *NodeMetricsTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, NodeMetricsTableName)
}

func (t *NodeMetricsTable) Insert(ctx *sql.Context, resource interface{}) error {
	metrics, ok := resource.(*v1beta1.NodeMetrics)
	if !ok {
		return fmt.Errorf("resource is not of type *v1beta1.NodeMetrics")
	}
	inserter := t.table.Inserter(ctx)
	defer inserter.Close(ctx)

	return inserter.Insert(ctx, nodeMetricsRow(metrics))
}

func nodeMetricsRow(metrics *v1beta1.NodeMetrics) sql.Row {
	return sql.NewRow(metrics.Name, metrics.Namespace, metrics.Window.Milliseconds(),
		metrics.Usage.Memory().Value(),
		metrics.Usage.Cpu().MilliValue(),
		metrics.Usage.StorageEphemeral().Value(),
		metrics.CreationTimestamp.Time)
}

func (t *NodeMetricsTable) Delete(ctx *sql.Context, resource interface{}) error {
	metrics, ok := resource.(*v1beta1.NodeMetrics)
	if !ok {
		return fmt.Errorf("resource is not of type *v1beta1.NodeMetrics")
	}
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return deleter.Delete(ctx, nodeMetricsRow(metrics))
}

func (t *NodeMetricsTable) Update(ctx *sql.Context, oldres, newres interface{}) error {
	oldMetrics, ok := oldres.(*v1beta1.NodeMetrics)
	if !ok {
		return fmt.Errorf("oldres is not of type *v1beta1.NodeMetrics")
	}
	newMetrics, ok := newres.(*v1beta1.NodeMetrics)
	if !ok {
		return fmt.Errorf("newres is not of type *v1beta1.NodeMetrics")
	}

	updater := t.table.Updater(ctx)
	defer updater.Close(ctx)

	return updater.Update(ctx, nodeMetricsRow(oldMetrics), nodeMetricsRow(newMetrics))
}

func onAddNodeMetrics(o interface{}) {
	t := table(NodeMetricsTableName)
	metrics := o.(*v1beta1.NodeMetrics)
	t.Log().Debugf("adding pod: %s\n", metrics.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Insert(ctx, metrics); err != nil {
		t.Log().Error(err)
	}
}

func onDelNodeMetrics(o interface{}) {
	t := table(NodeMetricsTableName)
	handleMetrics := func(metrics *v1beta1.NodeMetrics) {
		t.Log().Debugf("deleting pod: %s\n", metrics.Name)
		ctx := sql.NewEmptyContext()
		if err := t.Delete(ctx, metrics); err != nil {
			t.Log().Error(err)
		}
	}

	switch v := o.(type) {
	case *v1beta1.NodeMetrics:
		metrics := o.(*v1beta1.NodeMetrics)
		handleMetrics(metrics)
	case cache.DeletedFinalStateUnknown:
		deletedFinalStateUnknown := o.(cache.DeletedFinalStateUnknown)
		metrics := deletedFinalStateUnknown.Obj.(*v1beta1.NodeMetrics)
		handleMetrics(metrics)
	default:
		t.Log().Error("cannot handle deleted object of type %T", v)
	}
}

func onUpdateNodeMetrics(oldObj interface{}, newObj interface{}) {
	t := table(NodeMetricsTableName)
	oldMetrics := oldObj.(*v1beta1.NodeMetrics)
	newMetrics := newObj.(*v1beta1.NodeMetrics)
	t.Log().Debugf("updating pod: %s\n", oldMetrics.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Update(ctx, oldMetrics, newMetrics); err != nil {
		t.Log().Error(err)
	}
}
