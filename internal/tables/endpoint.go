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

const endpointTableName = "Endpoint"

var (
	endpointTable *EndpointTable
	endpointLog   = logrus.WithFields(
		logrus.Fields{
			"table": endpointTableName,
		},
	)
)

func StartEndpointInformer(ctx context.Context, db *memory.Database) {

	factory := informers.NewSharedInformerFactory(services.Clientset, 0)
	informer := factory.Core().V1().Endpoints().Informer()

	defer runtime.HandleCrash()

	// start informer ->
	go factory.Start(ctx.Done())

	// start to sync and call list
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	intiEndpointTable(db, informer)
	// informer event handler
	endpointTable.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAddEndpoint,
		UpdateFunc: onUpdateEndpoint,
		DeleteFunc: onDelEndpoint,
	})
	<-ctx.Done()
}

type EndpointTable struct {
	db       *memory.Database
	table    *memory.Table
	informer cache.SharedIndexInformer
}

func intiEndpointTable(db *memory.Database, informer cache.SharedIndexInformer) {
	if endpointTable != nil {
		endpointLog.Warn("endpointTable name is empty")
		return
	}
	endpointTable = &EndpointTable{
		db:       db,
		table:    createEndpointTable(db),
		informer: informer,
	}
}

func createEndpointTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(endpointTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "uid", Type: sql.Text, Nullable: false, Source: endpointTableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: endpointTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: endpointTableName},
		{Name: "hostname", Type: sql.Text, Nullable: false, Source: endpointTableName},
		{Name: "ip", Type: sql.Text, Nullable: false, Source: endpointTableName},
		{Name: "portname", Type: sql.Text, Nullable: false, Source: endpointTableName},
		{Name: "port", Type: sql.Int32, Nullable: false, Source: endpointTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: endpointTableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(endpointTableName, table)
	endpointLog.Infof("table [%s] created", endpointTableName)
	return table
}

func (t *EndpointTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, PodTableName)
}

func (t *EndpointTable) Insert(ctx *sql.Context, svc *v1.Endpoints) error {
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
					endpointLog.Error(err)
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

func (t *EndpointTable) Delete(ctx *sql.Context, svc *v1.Endpoints) error {
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return transverseEndpoints(ctx, svc, deleter.StatementBegin, deleter.StatementComplete, deleter.Delete, deleter.DiscardChanges)
}

func (t *EndpointTable) Update(ctx *sql.Context, oldSvc, newSvc *v1.Endpoints) error {
	if err := t.Delete(ctx, oldSvc); err != nil {
		return err
	}
	if err := t.Insert(ctx, newSvc); err != nil {
		return err
	}
	return nil
}

func onAddEndpoint(o interface{}) {
	endpoints := o.(*v1.Endpoints)
	endpointLog.Debugf("adding endpoint: %s\n", endpoints.Name)
	ctx := sql.NewEmptyContext()
	if err := endpointTable.Insert(ctx, endpoints); err != nil {
		fmt.Printf("%v\n", err)
		endpointLog.Error(err)
	}
}

func onDelEndpoint(o interface{}) {
	endpoints := o.(*v1.Endpoints)
	endpointLog.Debugf("deleting endpoint: %s\n", endpoints.Name)
	ctx := sql.NewEmptyContext()
	if err := endpointTable.Delete(ctx, endpoints); err != nil {
		endpointLog.Error(err)
	}
}

func onUpdateEndpoint(oldObj interface{}, newObj interface{}) {
	old := oldObj.(*v1.Endpoints)
	new := newObj.(*v1.Endpoints)
	endpointLog.Debugf("updating endpoint: %s\n", old.Name)

	ctx := sql.NewEmptyContext()
	if err := endpointTable.Update(ctx, old, new); err != nil {
		endpointLog.Error(err)
	}
}
