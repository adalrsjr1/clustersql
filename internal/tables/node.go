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
	nodeTable *NodeTable
	nodeLog   = logrus.StandardLogger()
)

const nodeTableName = "Node"

func StartNodeInformer(ctx context.Context, db *memory.Database) {

	factory := informers.NewSharedInformerFactory(services.Clientset, 0)
	informer := factory.Core().V1().Nodes().Informer()

	defer runtime.HandleCrash()

	// start informer ->
	go factory.Start(ctx.Done())

	// start to sync and call list
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	initNodeTable(db, informer)
	// informer event handler
	nodeTable.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAddNode,
		UpdateFunc: onUpdateNode,
		DeleteFunc: onDelNode,
	})
	<-ctx.Done()
}

type NodeTable struct {
	db       *memory.Database
	table    *memory.Table
	informer cache.SharedIndexInformer
}

func initNodeTable(db *memory.Database, informer cache.SharedIndexInformer) {
	if nodeTable != nil {
		nodeLog.Warn("endpointTable name is empty")
		return
	}
	nodeTable = &NodeTable{
		db:       db,
		table:    createNodetable(db),
		informer: informer,
	}
}

func createNodetable(db *memory.Database) *memory.Table {
	table := memory.NewTable(nodeTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "uid", Type: sql.Text, Nullable: false, Source: nodeTableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: nodeTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: nodeTableName},
		{Name: "free_memory", Type: sql.Int64, Nullable: false, Source: nodeTableName},
		{Name: "free_cpu", Type: sql.Int64, Nullable: false, Source: nodeTableName},
		{Name: "free_disk", Type: sql.Int64, Nullable: false, Source: nodeTableName},
		{Name: "capacity_memory", Type: sql.Int64, Nullable: false, Source: nodeTableName},
		{Name: "capacity_cpu", Type: sql.Int64, Nullable: false, Source: nodeTableName},
		{Name: "capacity_disk", Type: sql.Int64, Nullable: false, Source: nodeTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: nodeTableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(nodeTableName, table)
	nodeLog.Infof("table [%s] created", nodeTableName)
	return table
}

func (t *NodeTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, nodeTableName)
}

func (t *NodeTable) Insert(ctx *sql.Context, node *v1.Node) error {
	inserter := t.table.Inserter(ctx)
	defer inserter.Close(ctx)

	return inserter.Insert(ctx, nodeRow(node))
}

func nodeRow(node *v1.Node) sql.Row {
	return sql.NewRow(string(node.UID), node.Name, node.Namespace, node.Status.Allocatable.Memory().Value(), node.Status.Allocatable.Cpu().MilliValue(),
		node.Status.Allocatable.StorageEphemeral().Value(), node.Status.Capacity.Memory().Value(), node.Status.Capacity.Cpu().MilliValue(), node.Status.Capacity.StorageEphemeral().Value(),
		node.CreationTimestamp.Time)
}

func (t *NodeTable) Delete(ctx *sql.Context, node *v1.Node) error {
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return deleter.Delete(ctx, nodeRow(node))
}

func (t *NodeTable) Update(ctx *sql.Context, oldNode, newNode *v1.Node) error {

	updater := t.table.Updater(ctx)
	return updater.Update(ctx, nodeRow(oldNode), nodeRow(newNode))
}

func onAddNode(o interface{}) {
	endpoints := o.(*v1.Node)
	nodeLog.Debugf("adding endpoint: %s\n", endpoints.Name)
	ctx := sql.NewEmptyContext()
	if err := nodeTable.Insert(ctx, endpoints); err != nil {
		fmt.Printf("%v\n", err)
		nodeLog.Error(err)
	}
}

func onDelNode(o interface{}) {
	endpoints := o.(*v1.Node)
	nodeLog.Debugf("deleting endpoint: %s\n", endpoints.Name)
	ctx := sql.NewEmptyContext()
	if err := nodeTable.Delete(ctx, endpoints); err != nil {
		nodeLog.Error(err)
	}
}

func onUpdateNode(oldObj interface{}, newObj interface{}) {
	old := oldObj.(*v1.Node)
	new := newObj.(*v1.Node)
	nodeLog.Debugf("updating endpoint: %s\n", old.Name)

	ctx := sql.NewEmptyContext()
	if err := nodeTable.Update(ctx, old, new); err != nil {
		nodeLog.Error(err)
	}
}
