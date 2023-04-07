package tables

import (
	"context"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

func StartContainerInformer(ctx context.Context, db *memory.Database) {

	informerConstructor := func(factory informers.SharedInformerFactory) cache.SharedIndexInformer {
		return factory.Core().V1().Pods().Informer()
	}

	startResourceInformer(ctx, db, informerConstructor, initContainerTable, onAddContainer, onUpdateContainer, onDelContainer)

}

type ContainerTable struct {
	db     *memory.Database
	table  *memory.Table
	logger *logrus.Entry
}

func initContainerTable(db *memory.Database) {
	if _, ok := tables[ContainerTableName]; !ok {
		tables[ContainerTableName] = &ContainerTable{
			db:     db,
			table:  createContainerTable(db),
			logger: tableLogger(ContainerTableName),
		}
	}

}

func createContainerTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(ContainerTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "pod_uid", Type: sql.Text, Nullable: false, Source: ContainerTableName},
		{Name: "pod", Type: sql.Text, Nullable: false, Source: ContainerTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: ContainerTableName},
		{Name: "container", Type: sql.Text, Nullable: false, Source: ContainerTableName},
		{Name: "limit_memory", Type: sql.Int64, Nullable: false, Source: ContainerTableName},
		{Name: "limit_cpu", Type: sql.Int64, Nullable: false, Source: ContainerTableName},
		{Name: "limit_disk", Type: sql.Int64, Nullable: false, Source: ContainerTableName},
		{Name: "request_memory", Type: sql.Int64, Nullable: false, Source: ContainerTableName},
		{Name: "request_cpu", Type: sql.Int64, Nullable: false, Source: ContainerTableName},
		{Name: "request_disk", Type: sql.Int64, Nullable: false, Source: ContainerTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: ContainerTableName},
	}), db.GetForeignKeyCollection())

	db.AddTable(ContainerTableName, table)
	log.Infof("table [%s] created", ContainerTableName)
	return table
}

func (t *ContainerTable) Log() *logrus.Entry {
	return t.logger
}

func (t *ContainerTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, ContainerTableName)
}

func (t *ContainerTable) Insert(ctx *sql.Context, resource interface{}) error {
	pod := resource.(*v1.Pod)
	inserter := t.table.Inserter(ctx)
	defer inserter.Close(ctx)

	return transverseContainers(ctx, pod, inserter.StatementBegin, inserter.StatementComplete, inserter.Insert, inserter.DiscardChanges)
}

func transverseContainers(ctx *sql.Context, pod *v1.Pod,
	closureBegin func(*sql.Context),
	closureComplete func(*sql.Context) error,
	closureAction func(*sql.Context, sql.Row) error,
	closureDiscard func(*sql.Context, error) error) error {

	containers := pod.Spec.Containers
	closureBegin(ctx)

	for _, container := range containers {
		err := closureAction(ctx, containerRow(pod, &container))
		if err != nil {
			log.Error(err)
			return closureDiscard(ctx, err)
		}
	}

	return closureComplete(ctx)
}

func containerRow(pod *v1.Pod, container *v1.Container) sql.Row {
	return sql.NewRow(string(pod.UID), pod.Name, pod.Namespace, container.Name,
		container.Resources.Limits.Memory().Value(),
		container.Resources.Limits.Cpu().MilliValue(),
		container.Resources.Limits.StorageEphemeral().Value(),
		container.Resources.Requests.Memory().Value(),
		container.Resources.Requests.Cpu().MilliValue(),
		container.Resources.Requests.StorageEphemeral().Value(),
		pod.CreationTimestamp.Time)
}

func (t *ContainerTable) Delete(ctx *sql.Context, resource interface{}) error {
	pod := resource.(*v1.Pod)
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return transverseContainers(ctx, pod, deleter.StatementBegin, deleter.StatementComplete, deleter.Delete, deleter.DiscardChanges)
}

func (t *ContainerTable) Update(ctx *sql.Context, oldres, newres interface{}) error {
	oldPod := oldres.(*v1.Pod)
	newPod := newres.(*v1.Pod)
	if err := t.Delete(ctx, oldPod); err != nil {
		return err
	}
	if err := t.Insert(ctx, newPod); err != nil {
		return err
	}
	return nil
}

func onAddContainer(o interface{}) {
	t := table(ContainerTableName)
	pod := o.(*v1.Pod)
	log.Debugf("adding pod: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Insert(ctx, pod); err != nil {
		t.Log().Error(err)
	}
}

func onDelContainer(o interface{}) {
	t := table(ContainerTableName)
	pod := o.(*v1.Pod)
	log.Debugf("deleting pod: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Delete(ctx, pod); err != nil {
		t.Log().Error(err)
	}
}

func onUpdateContainer(oldObj interface{}, newObj interface{}) {
	t := table(ContainerTableName)
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)
	log.Debugf("updating pod: %s\n", oldPod.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Update(ctx, oldPod, newPod); err != nil {
		t.Log().Error(err)
	}
}
