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
	// go tb.StartNodeMetricsInformer(informersCtx, db)
	// go tb.StartPodMetricsInformer(informersCtx, db)
	// go tb.StartPodInformer(informersCtx, db)
	// // // TEST pod x endpoint: SELECT Pods.name AS pod, Endpoints.name AS endpoint, Pods.ip AS pod_ip , Endpoints.ip AS endpoint_IP FROM Pods LEFT JOIN Endpoints ON Pods.ip=Endpoints.ip ORDER BY Pods.name, Endpoints.name ASC
	// go tb.StartEndpointInformer(informersCtx, db)
	// go tb.StartNodeInformer(informersCtx, db)
	// go tb.StartContainerInformer(informersCtx, db)
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

// func createTestDatabase(ctx *sql.Context) *memory.Database {
// 	// mysql --host=0.0.0.0 --port=3306 --user=root mydb --execute="SELECT * FROM mytable;"
// 	db := memory.NewDatabase(dbName)
// 	table := memory.NewTable(tableName, sql.NewPrimaryKeySchema(sql.Schema{
// 		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName, PrimaryKey: true},
// 		{Name: "email", Type: sql.Text, Nullable: false, Source: tableName, PrimaryKey: true},
// 		{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
// 		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: tableName},
// 	}), db.GetForeignKeyCollection())
// 	db.AddTable(tableName, table)

// 	creationTime := time.Unix(0, 1667304000000001000).UTC()
// 	_ = table.Insert(ctx, sql.NewRow("Jane Deo", "janedeo@gmail.com", sql.MustJSON(`["556-565-566", "777-777-777"]`), creationTime))
// 	_ = table.Insert(ctx, sql.NewRow("Jane Doe", "jane@doe.com", sql.MustJSON(`[]`), creationTime))
// 	_ = table.Insert(ctx, sql.NewRow("John Doe", "john@doe.com", sql.MustJSON(`["555-555-555"]`), creationTime))
// 	_ = table.Insert(ctx, sql.NewRow("John Doe", "johnalt@doe.com", sql.MustJSON(`[]`), creationTime))

// 	return db
// }
