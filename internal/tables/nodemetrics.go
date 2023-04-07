package tables

import (
	"context"
	"fmt"

	"github.com/adalrsjr1/sqlcluster/internal/services"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

const nodeMetricsTableName = "Node_Metrics"

var (
	nodeMetricsTable *NodeMetricsTable
	nodeMetricsLog   = logrus.WithFields(
		logrus.Fields{
			"table": nodeMetricsTableName,
		},
	)
)

func StartNodeMetricsInformer(ctx context.Context, db *memory.Database) {
	watchList := cache.NewListWatchFromClient(services.ClientsetVS.MetricsV1beta1().RESTClient(),
		"nodes", v1.NamespaceAll, fields.Everything())

	_, informer := cache.NewInformer(
		watchList,
		&v1beta1.NodeMetrics{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    onAddNodeMetrics,
			UpdateFunc: onUpdateNodeMetrics,
			DeleteFunc: onDelNodeMetrics,
		},
	)

	defer runtime.HandleCrash()

	initNodeMetricsTable(db, informer)
	go informer.Run(ctx.Done())
	// start to sync and call list
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	<-ctx.Done()
}

type NodeMetricsTable struct {
	db       *memory.Database
	table    *memory.Table
	informer cache.Controller
}

func initNodeMetricsTable(db *memory.Database, informer cache.Controller) {
	if nodeMetricsTable != nil {
		nodeMetricsLog.Warn("table name is empty")
		return
	}
	nodeMetricsTable = &NodeMetricsTable{
		db:       db,
		table:    createNodeMetricsTable(db),
		informer: informer,
	}

}

func createNodeMetricsTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(nodeMetricsTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: nodeMetricsTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: nodeMetricsTableName},
		{Name: "window", Type: sql.Int64, Nullable: false, Source: nodeMetricsTableName},
		{Name: "usage_memory", Type: sql.Int64, Nullable: false, Source: nodeMetricsTableName},
		{Name: "usage_cpu", Type: sql.Int64, Nullable: false, Source: nodeMetricsTableName},
		{Name: "usage_disk", Type: sql.Int64, Nullable: false, Source: nodeMetricsTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: nodeMetricsTableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(nodeMetricsTableName, table)
	nodeMetricsLog.Infof("table [%s] created", nodeMetricsTableName)
	return table
}

func (t *NodeMetricsTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, nodeMetricsTableName)
}

func (t *NodeMetricsTable) Insert(ctx *sql.Context, metrics *v1beta1.NodeMetrics) error {
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

func (t *NodeMetricsTable) Delete(ctx *sql.Context, metrics *v1beta1.NodeMetrics) error {
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return deleter.Delete(ctx, nodeMetricsRow(metrics))
}

func (t *NodeMetricsTable) Update(ctx *sql.Context, oldNodeMetrics, newNodeMetrics *v1beta1.NodeMetrics) error {
	updater := t.table.Updater(ctx)
	defer updater.Close(ctx)

	return updater.Update(ctx, nodeMetricsRow(oldNodeMetrics), nodeMetricsRow(newNodeMetrics))
}

func onAddNodeMetrics(o interface{}) {
	metrics := o.(*v1beta1.NodeMetrics)
	nodeMetricsLog.Debugf("adding pod: %s\n", metrics.Name)
	ctx := sql.NewEmptyContext()
	if err := nodeMetricsTable.Insert(ctx, metrics); err != nil {
		nodeMetricsLog.Error(err)
	}
}

func onDelNodeMetrics(o interface{}) {
	handleMetrics := func(metrics *v1beta1.NodeMetrics) {
		nodeMetricsLog.Debugf("deleting pod: %s\n", metrics.Name)
		ctx := sql.NewEmptyContext()
		if err := nodeMetricsTable.Delete(ctx, metrics); err != nil {
			nodeMetricsLog.Error(err)
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
		nodeMetricsLog.Error("cannot handle deleted object of type %T", v)
	}
}

func onUpdateNodeMetrics(oldObj interface{}, newObj interface{}) {
	oldMetrics := oldObj.(*v1beta1.NodeMetrics)
	newMetrics := newObj.(*v1beta1.NodeMetrics)
	nodeMetricsLog.Debugf("updating pod: %s\n", oldMetrics.Name)
	ctx := sql.NewEmptyContext()
	if err := nodeMetricsTable.Update(ctx, oldMetrics, newMetrics); err != nil {
		nodeMetricsLog.Error(err)
	}
}
