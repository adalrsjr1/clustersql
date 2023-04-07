package tables

import (
	"context"
	"fmt"

	"github.com/adalrsjr1/sqlcluster/internal/services"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

func startResourceInformer(ctx context.Context, db *memory.Database,
	informerConstructor func(factory informers.SharedInformerFactory) cache.SharedIndexInformer,
	initTable func(db *memory.Database),
	addFunc func(interface{}), updateFunc func(interface{}, interface{}), deleteFunc func(interface{})) {
	factory := informers.NewSharedInformerFactory(services.Clientset, 0)
	informer := informerConstructor(factory)

	defer runtime.HandleCrash()

	// start informer
	go factory.Start(ctx.Done())

	// wait for caches to sync
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	initTable(db)

	// informer event handler
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    addFunc,
		UpdateFunc: updateFunc,
		DeleteFunc: deleteFunc,
	})

	<-ctx.Done()
}

func tableLogger(table string) *logrus.Entry {
	return logrus.New().WithField("table", AffinityTableName)
}

var (
	tables = map[string]Table{}
	log    = *logrus.New().WithField("pkg", "tables")
)

func table(name string) Table {
	if t, ok := tables[name]; ok {
		return t
	}

	log.Warnf("table %s does not exists, returning nil", name)
	return nil
}

type Table interface {
	Drop(ctx *sql.Context) error
	Insert(ctx *sql.Context, resource interface{}) error
	Delete(ctx *sql.Context, resource interface{}) error
	Update(ctx *sql.Context, oldres, newres interface{}) error
	Log() *logrus.Entry
}
