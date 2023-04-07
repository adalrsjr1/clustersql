package tables

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

func StartPodInformer(ctx context.Context, db *memory.Database) {

	informerConstructor := func(factory informers.SharedInformerFactory) cache.SharedIndexInformer {
		return factory.Core().V1().Pods().Informer()
	}

	startResourceInformer(ctx, db, informerConstructor, initPodTable, onAddPod, onUpdatePod, onDelPod)

}

type PodTable struct {
	db     *memory.Database
	table  *memory.Table
	logger *logrus.Entry
}

func initPodTable(db *memory.Database) {
	if _, ok := tables[PodTableName]; !ok {
		tables[PodTableName] = &PodTable{
			db:     db,
			table:  createPodTable(db),
			logger: tableLogger(PodTableName),
		}
	}
}

func createPodTable(db *memory.Database) *memory.Table {

	table := memory.NewTable(PodTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "uid", Type: sql.Text, Nullable: false, Source: PodTableName, PrimaryKey: true},
		{Name: "name", Type: sql.Text, Nullable: false, Source: PodTableName, PrimaryKey: true},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: PodTableName},
		{Name: "application", Type: sql.Text, Nullable: false, Source: PodTableName},
		{Name: "deployment", Type: sql.Text, Nullable: false, Source: PodTableName},
		{Name: "node", Type: sql.Text, Nullable: false, Source: PodTableName},
		{Name: "ip", Type: sql.Text, Nullable: false, Source: PodTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: PodTableName},
	}), db.GetForeignKeyCollection())

	db.AddTable(PodTableName, table)
	log.Infof("table [%s] created", PodTableName)
	return table
}

func (t *PodTable) Log() *logrus.Entry {
	return t.logger
}

func (t *PodTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, PodTableName)
}

func (t *PodTable) Insert(ctx *sql.Context, resource interface{}) error {
	pod, ok := resource.(*v1.Pod)
	if !ok {
		return fmt.Errorf("unexpected type for resource, expected *v1.Pod but got %T", resource)
	}
	return t.table.Insert(ctx, podRow(pod))
}

func podRow(pod *v1.Pod) sql.Row {
	name := pod.Name
	tokens := strings.Split(name, "-")
	deploymentName := ""
	if len(tokens) >= 3 {
		deploymentName = strings.Join(tokens[:len(tokens)-2], "-")
	}

	labels := pod.GetLabels()
	app := labels["app"]

	return sql.NewRow(string(pod.UID), pod.Name, pod.Namespace, app, deploymentName, pod.Spec.NodeName,
		pod.Status.PodIP, pod.CreationTimestamp.Time)
}

func (t *PodTable) Delete(ctx *sql.Context, resource interface{}) error {
	pod, ok := resource.(*v1.Pod)
	if !ok {
		return fmt.Errorf("unexpected type for resource, expected *v1.Pod but got %T", resource)
	}
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)
	return deleter.Delete(ctx, podRow(pod))
}

func (t *PodTable) Update(ctx *sql.Context, oldres interface{}, newres interface{}) error {
	oldPod, ok := oldres.(*v1.Pod)
	if !ok {
		return fmt.Errorf("unexpected type for oldResource, expected *v1.Pod but got %T", oldres)
	}
	newPod, ok := newres.(*v1.Pod)
	if !ok {
		return fmt.Errorf("unexpected type for newResource, expected *v1.Pod but got %T", newres)
	}
	updater := t.table.Updater(ctx)
	defer updater.Close(ctx)
	return updater.Update(ctx, podRow(oldPod), podRow(newPod))
}

func onAddPod(o interface{}) {
	t := table(PodTableName)
	pod := o.(*v1.Pod)
	t.Log().Debugf("adding pod: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Insert(ctx, pod); err != nil {
		t.Log().Error(err)
	}
}

func onDelPod(o interface{}) {
	t := table(PodTableName)
	pod := o.(*v1.Pod)
	t.Log().Debugf("deleting pod: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Delete(ctx, pod); err != nil {
		t.Log().Error(err)
	}
}

func onUpdatePod(oldObj interface{}, newObj interface{}) {
	t := table(PodTableName)
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)
	t.Log().Debugf("updating pod: %s\n", oldPod.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Update(ctx, oldPod, newPod); err != nil {
		t.Log().Error(err)
	}
}
