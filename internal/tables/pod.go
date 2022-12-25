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

var (
	podTable *PodTable
	podLog   = logrus.StandardLogger()
)

const podTableName = "Pod"

func StartPodInformer(ctx context.Context, db *memory.Database) {
	factory := informers.NewSharedInformerFactory(services.Clientset, 0)
	informer := factory.Core().V1().Pods().Informer()

	defer runtime.HandleCrash()

	// start informer ->
	go factory.Start(ctx.Done())

	// start to sync and call list
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	initPodTable(db, informer)
	// informer event handler
	podTable.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAddPod,
		UpdateFunc: onUpdatePod,
		DeleteFunc: onDelPod,
	})

	<-ctx.Done()
}

type PodTable struct {
	db       *memory.Database
	table    *memory.Table
	informer cache.SharedIndexInformer
}

func initPodTable(db *memory.Database, informer cache.SharedIndexInformer) {
	if podTable != nil {
		endpointLog.Warn("podTable name is empty")
		return
	}
	podTable = &PodTable{
		db:       db,
		table:    createPodTable(db),
		informer: informer,
	}
}

func createPodTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(podTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: sql.Text, Nullable: false, Source: podTableName, PrimaryKey: true},
		{Name: "name", Type: sql.Text, Nullable: false, Source: podTableName, PrimaryKey: true},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: podTableName},
		{Name: "node", Type: sql.Text, Nullable: false, Source: podTableName},
		{Name: "ip", Type: sql.Text, Nullable: false, Source: podTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: podTableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(podTableName, table)
	endpointLog.Infof("table [%s] created", podTableName)
	return table
}

func (t *PodTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, podTableName)
}

func (t *PodTable) Insert(ctx *sql.Context, pod *v1.Pod) error {
	return t.table.Insert(ctx, podRow(pod))
}

func podRow(pod *v1.Pod) sql.Row {
	return sql.NewRow(string(pod.UID), pod.Name, pod.Namespace, pod.Spec.NodeName, pod.Status.PodIP, pod.CreationTimestamp.Time)
}

func (t *PodTable) Delete(ctx *sql.Context, pod *v1.Pod) error {
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)
	return deleter.Delete(ctx, podRow(pod))
}

func (t *PodTable) Update(ctx *sql.Context, oldPod, newPod *v1.Pod) error {
	updater := t.table.Updater(ctx)
	defer updater.Close(ctx)
	return updater.Update(ctx, podRow(oldPod), podRow(newPod))
}

func onAddPod(o interface{}) {
	pod := o.(*v1.Pod)
	podLog.Debugf("adding pod: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := podTable.Insert(ctx, pod); err != nil {
		podLog.Error(err)
	}
}

func onDelPod(o interface{}) {
	pod := o.(*v1.Pod)
	podLog.Debugf("deleting pod: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := podTable.Delete(ctx, pod); err != nil {
		podLog.Error(err)
	}
}

func onUpdatePod(oldObj interface{}, newObj interface{}) {
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)
	podLog.Debugf("updating pod: %s\n", oldPod.Name)
	ctx := sql.NewEmptyContext()
	if err := podTable.Update(ctx, oldPod, newPod); err != nil {
		podLog.Error(err)
	}
}
