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

const podMetricsTableName = "Pod_Metrics"

var (
	podMetricsTable *PodMetricsTable
	podMetricsLog   = logrus.WithFields(
		logrus.Fields{
			"table": podMetricsTableName,
		},
	)
)

func StartPodMetricsInformer(ctx context.Context, db *memory.Database) {
	watchList := cache.NewListWatchFromClient(services.ClientsetVS.MetricsV1beta1().RESTClient(),
		"pods", v1.NamespaceAll, fields.Everything())

	_, informer := cache.NewInformer(
		watchList,
		&v1beta1.PodMetrics{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    onAddPodMetrics,
			UpdateFunc: onUpdatePodMetrics,
			DeleteFunc: onDelPodMetrics,
		},
	)

	defer runtime.HandleCrash()

	initPodMetricsTable(db, informer)
	go informer.Run(ctx.Done())
	// start to sync and call list
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	<-ctx.Done()
}

type PodMetricsTable struct {
	db       *memory.Database
	table    *memory.Table
	informer cache.Controller
}

func initPodMetricsTable(db *memory.Database, informer cache.Controller) {
	if podMetricsTable != nil {
		podMetricsLog.Warn("table name is empty")
		return
	}
	podMetricsTable = &PodMetricsTable{
		db:       db,
		table:    createPodMetricsTable(db),
		informer: informer,
	}

}

func createPodMetricsTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(podMetricsTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "pod", Type: sql.Text, Nullable: false, Source: podMetricsTableName},
		{Name: "container", Type: sql.Text, Nullable: false, Source: podMetricsTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: podMetricsTableName},
		{Name: "window", Type: sql.Int64, Nullable: false, Source: podMetricsTableName},
		{Name: "usage_memory", Type: sql.Int64, Nullable: false, Source: podMetricsTableName},
		{Name: "usage_cpu", Type: sql.Int64, Nullable: false, Source: podMetricsTableName},
		{Name: "usage_disk", Type: sql.Int64, Nullable: false, Source: podMetricsTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: podMetricsTableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(podMetricsTableName, table)
	podMetricsLog.Infof("table [%s] created", podMetricsTableName)
	return table
}

func (t *PodMetricsTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, podMetricsTableName)
}

func (t *PodMetricsTable) Insert(ctx *sql.Context, metrics *v1beta1.PodMetrics) error {
	inserter := t.table.Inserter(ctx)
	defer inserter.Close(ctx)

	return transverseContainersMetrics(ctx, metrics, inserter.StatementBegin, inserter.StatementComplete, inserter.Insert, inserter.DiscardChanges)
}

func transverseContainersMetrics(ctx *sql.Context, metrics *v1beta1.PodMetrics,
	closureBegin func(*sql.Context),
	closureComplete func(*sql.Context) error,
	closureAction func(*sql.Context, sql.Row) error,
	closureDiscard func(*sql.Context, error) error) error {

	containers := metrics.Containers
	closureBegin(ctx)

	for _, container := range containers {
		err := closureAction(ctx, podMetricsRow(metrics, &container))
		if err != nil {
			podMetricsLog.Error(err)
			return closureDiscard(ctx, err)
		}
	}

	return closureComplete(ctx)
}

func podMetricsRow(metrics *v1beta1.PodMetrics, container_metrics *v1beta1.ContainerMetrics) sql.Row {
	return sql.NewRow(metrics.Name, container_metrics.Name, metrics.Namespace, metrics.Window.Milliseconds(),
		container_metrics.Usage.Memory().Value(),
		container_metrics.Usage.Cpu().MilliValue(),
		container_metrics.Usage.StorageEphemeral().Value(),
		metrics.CreationTimestamp.Time)
}

func (t *PodMetricsTable) Delete(ctx *sql.Context, metrics *v1beta1.PodMetrics) error {
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return transverseContainersMetrics(ctx, metrics, deleter.StatementBegin, deleter.StatementComplete, deleter.Delete, deleter.DiscardChanges)
}

func (t *PodMetricsTable) Update(ctx *sql.Context, oldNodeMetrics, newNodeMetrics *v1beta1.PodMetrics) error {
	if err := t.Delete(ctx, oldNodeMetrics); err != nil {
		return err
	}
	if err := t.Insert(ctx, newNodeMetrics); err != nil {
		return err
	}
	return nil
}

func onAddPodMetrics(o interface{}) {
	metrics := o.(*v1beta1.PodMetrics)
	podMetricsLog.Debugf("adding pod: %s\n", metrics.Name)
	ctx := sql.NewEmptyContext()
	if err := podMetricsTable.Insert(ctx, metrics); err != nil {
		podMetricsLog.Error(err)
	}
}

func onDelPodMetrics(o interface{}) {

	handleMetrics := func(metrics *v1beta1.PodMetrics) {
		podMetricsLog.Debugf("deleting pod: %s\n", metrics.Name)
		ctx := sql.NewEmptyContext()
		if err := podMetricsTable.Delete(ctx, metrics); err != nil {
			podMetricsLog.Error(err)
		}
	}

	switch v := o.(type) {
	case *v1beta1.PodMetrics:
		metrics := o.(*v1beta1.PodMetrics)
		handleMetrics(metrics)
	case cache.DeletedFinalStateUnknown:
		deletedFinalStateUnknown := o.(cache.DeletedFinalStateUnknown)
		metrics := deletedFinalStateUnknown.Obj.(*v1beta1.PodMetrics)
		handleMetrics(metrics)
	default:
		podMetricsLog.Error("cannot handle deleted object of type %T", v)
	}

}

func onUpdatePodMetrics(oldObj interface{}, newObj interface{}) {
	oldMetrics := oldObj.(*v1beta1.PodMetrics)
	newMetrics := newObj.(*v1beta1.PodMetrics)
	podMetricsLog.Debugf("updating pod: %s\n", oldMetrics.Name)
	ctx := sql.NewEmptyContext()
	if err := podMetricsTable.Update(ctx, oldMetrics, newMetrics); err != nil {
		podMetricsLog.Error(err)
	}
}
