package tables

import (
	"context"
	"errors"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"k8s.io/client-go/tools/cache"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

func StartPodMetricsInformer(ctx context.Context, db *memory.Database) {
	startMetricsInformer(ctx, db, &v1beta1.PodMetrics{}, "pods", initPodMetricsTable, onAddPodMetrics, onUpdatePodMetrics, onDelPodMetrics)
}

type PodMetricsTable struct {
	db     *memory.Database
	table  *memory.Table
	logger *logrus.Entry
}

func initPodMetricsTable(db *memory.Database) {
	if _, ok := tables[PodMetricsTableName]; !ok {
		tables[PodMetricsTableName] = &PodMetricsTable{
			db:     db,
			table:  createPodMetricsTable(db),
			logger: tableLogger(PodMetricsTableName),
		}
	}

}

func createPodMetricsTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(PodMetricsTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "pod", Type: sql.Text, Nullable: false, Source: PodMetricsTableName},
		{Name: "container", Type: sql.Text, Nullable: false, Source: PodMetricsTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: PodMetricsTableName},
		{Name: "window", Type: sql.Int64, Nullable: false, Source: PodMetricsTableName},
		{Name: "usage_memory", Type: sql.Int64, Nullable: false, Source: PodMetricsTableName},
		{Name: "usage_cpu", Type: sql.Int64, Nullable: false, Source: PodMetricsTableName},
		{Name: "usage_disk", Type: sql.Int64, Nullable: false, Source: PodMetricsTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: PodMetricsTableName},
	}), db.GetForeignKeyCollection())

	db.AddTable(PodMetricsTableName, table)
	log.Infof("table [%s] created", PodMetricsTableName)
	return table
}

func (t *PodMetricsTable) Log() *logrus.Entry {
	return t.logger
}

func (t *PodMetricsTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, PodMetricsTableName)
}

func (t *PodMetricsTable) Insert(ctx *sql.Context, resource interface{}) error {
	metrics, ok := resource.(*v1beta1.PodMetrics)
	if !ok {
		return errors.New("resource is not of type *v1beta1.PodMetrics")
	}
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
			log.Error(err)
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

func (t *PodMetricsTable) Delete(ctx *sql.Context, resource interface{}) error {
	metrics, ok := resource.(*v1beta1.PodMetrics)
	if !ok {
		return errors.New("resource is not of type *v1beta1.PodMetrics")
	}
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return transverseContainersMetrics(ctx, metrics, deleter.StatementBegin, deleter.StatementComplete, deleter.Delete, deleter.DiscardChanges)
}

func (t *PodMetricsTable) Update(ctx *sql.Context, oldres, newres interface{}) error {
	oldMetrics, ok := oldres.(*v1beta1.PodMetrics)
	if !ok {
		return errors.New("oldres is not of type *v1beta1.PodMetrics")
	}
	newMetrics, ok := newres.(*v1beta1.PodMetrics)
	if !ok {
		return errors.New("newres is not of type *v1beta1.PodMetrics")
	}

	if err := t.Delete(ctx, oldMetrics); err != nil {
		return err
	}
	if err := t.Insert(ctx, newMetrics); err != nil {
		return err
	}
	return nil
}

func onAddPodMetrics(o interface{}) {
	t := table(PodMetricsTableName)
	metrics := o.(*v1beta1.PodMetrics)
	t.Log().Debugf("adding pod: %s\n", metrics.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Insert(ctx, metrics); err != nil {
		t.Log().Error(err)
	}
}

func onDelPodMetrics(o interface{}) {
	t := table(PodMetricsTableName)
	handleMetrics := func(metrics *v1beta1.PodMetrics) {
		t.Log().Debugf("deleting pod: %s\n", metrics.Name)
		ctx := sql.NewEmptyContext()
		if err := t.Delete(ctx, metrics); err != nil {
			t.Log().Error(err)
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
		t.Log().Error("cannot handle deleted object of type %T", v)
	}

}

func onUpdatePodMetrics(oldObj interface{}, newObj interface{}) {
	t := table(PodMetricsTableName)
	oldMetrics := oldObj.(*v1beta1.PodMetrics)
	newMetrics := newObj.(*v1beta1.PodMetrics)
	t.Log().Debugf("updating pod: %s\n", oldMetrics.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Update(ctx, oldMetrics, newMetrics); err != nil {
		t.Log().Error(err)
	}
}
