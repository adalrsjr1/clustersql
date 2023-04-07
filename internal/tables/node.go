package tables

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

func StartNodeInformer(ctx context.Context, db *memory.Database) {

	informerConstructor := func(factory informers.SharedInformerFactory) cache.SharedIndexInformer {
		return factory.Core().V1().Nodes().Informer()
	}

	startResourceInformer(ctx, db, informerConstructor, initNodeTable, onAddNode, onUpdateNode, onDelNode)

}

type NodeTable struct {
	db     *memory.Database
	table  *memory.Table
	logger *logrus.Entry
}

func initNodeTable(db *memory.Database) {
	if _, ok := tables[PodTableName]; !ok {
		tables[NodeTableName] = &NodeTable{
			db:     db,
			table:  createNodetable(db),
			logger: tableLogger(NodeTableName),
		}
	}

}

func createNodetable(db *memory.Database) *memory.Table {
	table := memory.NewTable(NodeTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "uid", Type: sql.Text, Nullable: false, Source: NodeTableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: NodeTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: NodeTableName},
		{Name: "free_memory", Type: sql.Int64, Nullable: false, Source: NodeTableName},
		{Name: "free_cpu", Type: sql.Int64, Nullable: false, Source: NodeTableName},
		{Name: "free_disk", Type: sql.Int64, Nullable: false, Source: NodeTableName},
		{Name: "capacity_memory", Type: sql.Int64, Nullable: false, Source: NodeTableName},
		{Name: "capacity_cpu", Type: sql.Int64, Nullable: false, Source: NodeTableName},
		{Name: "capacity_disk", Type: sql.Int64, Nullable: false, Source: NodeTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: NodeTableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(NodeTableName, table)
	log.Infof("table [%s] created", NodeTableName)
	return table
}

func (t *NodeTable) Log() *logrus.Entry {
	return t.logger
}

func (t *NodeTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, PodTableName)
}

func (t *NodeTable) Insert(ctx *sql.Context, resource interface{}) error {
	node := resource.(*v1.Node)
	inserter := t.table.Inserter(ctx)
	defer inserter.Close(ctx)

	return inserter.Insert(ctx, nodeRow(node))
}

func nodeRow(node *v1.Node) sql.Row {
	return sql.NewRow(string(node.UID), node.Name, node.Namespace, node.Status.Allocatable.Memory().Value(), node.Status.Allocatable.Cpu().MilliValue(),
		node.Status.Allocatable.StorageEphemeral().Value(), node.Status.Capacity.Memory().Value(), node.Status.Capacity.Cpu().MilliValue(), node.Status.Capacity.StorageEphemeral().Value(),
		node.CreationTimestamp.Time)
}

func (t *NodeTable) Delete(ctx *sql.Context, resource interface{}) error {
	node := resource.(*v1.Node)
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return deleter.Delete(ctx, nodeRow(node))
}

func (t *NodeTable) Update(ctx *sql.Context, oldres, newres interface{}) error {
	oldNode := oldres.(*v1.Node)
	newNode := newres.(*v1.Node)
	updater := t.table.Updater(ctx)
	return updater.Update(ctx, nodeRow(oldNode), nodeRow(newNode))
}

func onAddNode(o interface{}) {
	t := table(NodeTableName)
	node := o.(*v1.Node)
	log.Debugf("adding node: %s\n", node.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Insert(ctx, node); err != nil {
		fmt.Printf("%v\n", err)
		log.Error(err)
	}
}

func onDelNode(o interface{}) {
	t := table(NodeTableName)
	node := o.(*v1.Node)
	log.Debugf("deleting node: %s\n", node.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Delete(ctx, node); err != nil {
		log.Error(err)
	}
}

func onUpdateNode(oldObj interface{}, newObj interface{}) {
	t := table(NodeTableName)
	old := oldObj.(*v1.Node)
	new := newObj.(*v1.Node)
	log.Debugf("updating node: %s\n", old.Name)

	ctx := sql.NewEmptyContext()
	if err := t.Update(ctx, old, new); err != nil {
		log.Error(err)
	}
}
