package main

import (
	"fmt"

	"github.com/adalrsjr1/sqlcluster/internal/services"
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
	dbName  = "kubernetes"
	address = "0.0.0.0"
	port    = 3306
	log     = logrus.StandardLogger()
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	informersCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	services.StartKubernetes()

	db := memory.NewDatabase(dbName)

	engine := sqle.NewDefault(
		sql.NewDatabaseProvider(
			db,
			information_schema.NewInformationSchemaDatabase(),
		))

	go tb.StartAffinityInformer(informersCtx, db)
	go tb.StartNodeAffinityInformer(informersCtx, db)
	go tb.StartNodeMetricsInformer(informersCtx, db)
	go tb.StartPodMetricsInformer(informersCtx, db)
	go tb.StartPodInformer(informersCtx, db)
	go tb.StartEndpointInformer(informersCtx, db)
	go tb.StartNodeInformer(informersCtx, db)
	go tb.StartContainerInformer(informersCtx, db)
	go tb.StartTrafficInformer(informersCtx, db)
	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		log.Error(err)
	}
	if err = s.Start(); err != nil {
		log.Error(err)
	}
}
