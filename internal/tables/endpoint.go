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

func StartEndpointInformer(ctx context.Context, db *memory.Database) {

	informerConstructor := func(factory informers.SharedInformerFactory) cache.SharedIndexInformer {
		return factory.Core().V1().Endpoints().Informer()
	}

	startResourceInformer(ctx, db, informerConstructor, initEndpointTable, onAddEndpoint, onUpdateEndpoint, onDelEndpoint)
}

type EndpointTable struct {
	db     *memory.Database
	table  *memory.Table
	logger *logrus.Entry
}

func initEndpointTable(db *memory.Database) {
	if _, ok := tables[EndpointTableName]; !ok {
		tables[EndpointTableName] = &EndpointTable{
			db:     db,
			table:  createEndpointTable(db),
			logger: tableLogger(EndpointTableName),
		}
	}

}

func createEndpointTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(EndpointTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "uid", Type: sql.Text, Nullable: false, Source: EndpointTableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: EndpointTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: EndpointTableName},
		{Name: "hostname", Type: sql.Text, Nullable: false, Source: EndpointTableName},
		{Name: "ip", Type: sql.Text, Nullable: false, Source: EndpointTableName},
		{Name: "portname", Type: sql.Text, Nullable: false, Source: EndpointTableName},
		{Name: "port", Type: sql.Int32, Nullable: false, Source: EndpointTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: EndpointTableName},
	}), db.GetForeignKeyCollection())

	db.AddTable(EndpointTableName, table)
	log.Infof("table [%s] created", EndpointTableName)
	return table
}

func (t *EndpointTable) Log() *logrus.Entry {
	return t.logger
}

func (t *EndpointTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, EndpointTableName)
}

func (t *EndpointTable) Insert(ctx *sql.Context, resource interface{}) error {
	svc := resource.(*v1.Endpoints)
	inserter := t.table.Inserter(ctx)
	defer inserter.Close(ctx)

	return transverseEndpoints(ctx, svc, inserter.StatementBegin, inserter.StatementComplete, inserter.Insert, inserter.DiscardChanges)
}

func transverseEndpoints(ctx *sql.Context, svc *v1.Endpoints,
	closureBegin func(*sql.Context),
	closureComplete func(*sql.Context) error,
	closureAction func(*sql.Context, sql.Row) error,
	closureDiscard func(*sql.Context, error) error) error {

	endpoints := svc.Subsets
	closureBegin(ctx)

	for _, endpoint := range endpoints {
		addrs := endpoint.Addresses
		for _, addr := range addrs {
			for _, port := range endpoint.Ports {
				err := closureAction(ctx, svcRow(svc, &addr, &port))
				if err != nil {
					log.Error(err)
					return closureDiscard(ctx, err)
				}
			}

		}
	}

	return closureComplete(ctx)
}

func svcRow(svc *v1.Endpoints, addr *v1.EndpointAddress, port *v1.EndpointPort) sql.Row {
	return sql.NewRow(string(svc.UID), svc.Name, svc.Namespace, addr.Hostname, addr.IP, port.Name, port.Port, svc.CreationTimestamp.Time)
}

func (t *EndpointTable) Delete(ctx *sql.Context, resource interface{}) error {
	svc := resource.(*v1.Endpoints)
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return transverseEndpoints(ctx, svc, deleter.StatementBegin, deleter.StatementComplete, deleter.Delete, deleter.DiscardChanges)
}

func (t *EndpointTable) Update(ctx *sql.Context, oldres, newres interface{}) error {
	oldSvc := oldres.(*v1.Endpoints)
	newSvc := newres.(*v1.Endpoints)
	if err := t.Delete(ctx, oldSvc); err != nil {
		return err
	}
	if err := t.Insert(ctx, newSvc); err != nil {
		return err
	}
	return nil
}

func onAddEndpoint(o interface{}) {
	t := table(EndpointTableName)
	endpoints := o.(*v1.Endpoints)
	t.Log().Debugf("adding endpoint: %s\n", endpoints.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Insert(ctx, endpoints); err != nil {
		t.Log().Error(err)
	}
}

func onDelEndpoint(o interface{}) {
	t := table(EndpointTableName)
	endpoints := o.(*v1.Endpoints)
	t.Log().Debugf("deleting endpoint: %s\n", endpoints.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Delete(ctx, endpoints); err != nil {
		t.Log().Error(err)
	}
}

func onUpdateEndpoint(oldObj interface{}, newObj interface{}) {
	t := table(EndpointTableName)
	old := oldObj.(*v1.Endpoints)
	new := newObj.(*v1.Endpoints)
	t.Log().Debugf("updating endpoint: %s\n", old.Name)

	ctx := sql.NewEmptyContext()
	if err := t.Update(ctx, old, new); err != nil {
		t.Log().Error(err)
	}
}
