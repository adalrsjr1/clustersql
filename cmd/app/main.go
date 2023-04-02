package main

import (
	"flag"
	"fmt"

	"github.com/adalrsjr1/sqlcluster/internal/services"
	"github.com/adalrsjr1/sqlcluster/internal/tables"
	tb "github.com/adalrsjr1/sqlcluster/internal/tables"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var (
	dbName  string
	address string
	port    int
	log     = logrus.New().WithField("pkg", "main")
)

func init() {
	flag.StringVar(&dbName, "dbname", "kubernetes", "name of the database")
	flag.StringVar(&address, "address", "0.0.0.0", "address to bind the server to")
	flag.IntVar(&port, "port", 3306, "port to listen on")
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	services.StartKubernetes()

	db := memory.NewDatabase(dbName)
	dbProvider := sql.NewDatabaseProvider(db, information_schema.NewInformationSchemaDatabase())
	engine := sqle.NewDefault(dbProvider)

	runInformers(ctx, db)

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}

	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		log.WithError(err).Error("error creating server")
		return
	}

	go func() {
		<-ctx.Done()
		if err := s.Close(); err != nil {
			log.WithError(err).Error("error stopping server")
		} else {
			log.Info("server stopped")
		}
	}()

	if err = s.Start(); err != nil {
		log.WithError(err).Error("error starting server")
		return
	}

}

func runInformers(ctx context.Context, db *memory.Database) {
	tables := []struct {
		name      string
		startFunc func(context.Context, *memory.Database)
	}{
		{tables.AffinityTableName, tb.StartAffinityInformer},
		{tables.NodeAffinityTableName, tb.StartNodeAffinityInformer},
		{tables.NodeMetricsTableName, tb.StartNodeMetricsInformer},
		{tables.PodMetricsTableName, tb.StartPodMetricsInformer},
		{tables.PodTableName, tb.StartPodInformer},
		{tables.EndpointTableName, tb.StartEndpointInformer},
		{tables.NodeTableName, tb.StartNodeInformer},
		{tables.ContainerTableName, tb.StartContainerInformer},
		{tables.TrafficTableName, tb.StartTrafficInformer},
	}

	for _, t := range tables {
		tableCtx := sql.NewContext(ctx)
		log.Infof("starting informer: %s", t.name)
		go t.startFunc(tableCtx, db)
	}

}
