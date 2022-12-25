package tables

import (
	"context"
	"fmt"

	"github.com/adalrsjr1/sqlcluster/internal/services"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

const containerTableName = "Container"

var (
	containerTable *ContainerTable
	containerLog   = logrus.WithFields(
		logrus.Fields{
			"table": containerTableName,
		},
	)
)

func StartContainerInformer(ctx context.Context, db *memory.Database) {
	factory := informers.NewSharedInformerFactory(services.Clientset, 0)
	informer := factory.Core().V1().Pods().Informer()
	defer runtime.HandleCrash()

	// start informer ->
	go factory.Start(ctx.Done())

	// start to sync and call list
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	initContainertable(db, informer)
	// informer event handler
	containerTable.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAddContainer,
		UpdateFunc: onUpdateContainer,
		DeleteFunc: onDelContainer,
	})

	<-ctx.Done()
}

type ContainerTable struct {
	db       *memory.Database
	table    *memory.Table
	informer cache.SharedIndexInformer
}

func initContainertable(db *memory.Database, informer cache.SharedIndexInformer) {
	if containerTable != nil {
		containerLog.Warn("podTable name is empty")
		return
	}
	containerTable = &ContainerTable{
		db:       db,
		table:    createContainerTable(db),
		informer: informer,
	}
}

func createContainerTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(containerTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "pod_uid", Type: sql.Text, Nullable: false, Source: containerTableName},
		{Name: "pod", Type: sql.Text, Nullable: false, Source: containerTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: containerTableName},
		{Name: "container", Type: sql.Text, Nullable: false, Source: containerTableName},
		{Name: "limit_memory", Type: sql.Int64, Nullable: false, Source: containerTableName},
		{Name: "limit_cpu", Type: sql.Int64, Nullable: false, Source: containerTableName},
		{Name: "limit_disk", Type: sql.Int64, Nullable: false, Source: containerTableName},
		{Name: "request_memory", Type: sql.Int64, Nullable: false, Source: containerTableName},
		{Name: "request_cpu", Type: sql.Int64, Nullable: false, Source: containerTableName},
		{Name: "request_disk", Type: sql.Int64, Nullable: false, Source: containerTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: containerTableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(containerTableName, table)
	containerLog.Infof("table [%s] created", containerTableName)
	return table
}

func (t *ContainerTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, containerTableName)
}

func (t *ContainerTable) Insert(ctx *sql.Context, pod *v1.Pod) error {
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
			containerLog.Error(err)
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

func (t *ContainerTable) Delete(ctx *sql.Context, pod *v1.Pod) error {
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return transverseContainers(ctx, pod, deleter.StatementBegin, deleter.StatementComplete, deleter.Delete, deleter.DiscardChanges)
}

func (t *ContainerTable) Update(ctx *sql.Context, oldPod, newPod *v1.Pod) error {
	if err := t.Delete(ctx, oldPod); err != nil {
		return err
	}
	if err := t.Insert(ctx, newPod); err != nil {
		return err
	}
	return nil
}

func onAddContainer(o interface{}) {
	pod := o.(*v1.Pod)
	containerLog.Debugf("adding pod: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := containerTable.Insert(ctx, pod); err != nil {
		containerLog.Error(err)
	}
}

func onDelContainer(o interface{}) {
	pod := o.(*v1.Pod)
	containerLog.Debugf("deleting pod: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := containerTable.Delete(ctx, pod); err != nil {
		containerLog.Error(err)
	}
}

func onUpdateContainer(oldObj interface{}, newObj interface{}) {
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)
	containerLog.Debugf("updating pod: %s\n", oldPod.Name)
	ctx := sql.NewEmptyContext()
	if err := containerTable.Update(ctx, oldPod, newPod); err != nil {
		containerLog.Error(err)
	}
}
