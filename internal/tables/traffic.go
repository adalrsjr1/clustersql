package tables

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/runtime"
)

const (
	duration = "5m"
	// queries
	// istio metrics: https://istio.io/latest/docs/reference/config/metrics/
	requestCountQuery        = "rate(istio_requests_total[5m])"
	grpcMessageRequestQuery  = "rate(istio_request_messages_total[5m])"
	grpcMessageResponseQuery = "rate(istio_response_messages_total[5m])"
	requestDurationQuery     = "increase(istio_request_duration_milliseconds_sum[5m]) / increase(istio_request_duration_milliseconds_count[5m])"
	requestDuration50Query   = "histogram_quantile(.50, rate(istio_request_duration_milliseconds_bucket[5m]))"
	requestDuration95Query   = "histogram_quantile(.95, rate(istio_request_duration_milliseconds_bucket[5m]))"
	requestDuration99Query   = "histogram_quantile(.99, rate(istio_request_duration_milliseconds_bucket[5m]))"
	requestSizeQuery         = "increase(istio_request_bytes_sum[5m]) / increase(istio_request_bytes_count[5m])"
	requestSize50Query       = "histogram_quantile(.50, rate(istio_request_bytes_bucket[5m]))"
	requestSize95Query       = "histogram_quantile(.95, rate(istio_request_bytes_bucket[5m]))"
	requestSize99Query       = "histogram_quantile(.99, rate(istio_request_bytes_bucket[5m]))"
	responseSizeQuery        = "increase(istio_response_bytes_sum[5m]) / increase(istio_response_bytes_count[5m])"
	responseSize50Query      = "histogram_quantile(.50, rate(istio_response_bytes_bucket[5m]))"
	responseSize95Query      = "histogram_quantile(.95, rate(istio_response_bytes_bucket[5m]))"
	responseSize99Query      = "histogram_quantile(.99, rate(istio_response_bytes_bucket[5m]))"

	metricHttpRequest         = "http_request"
	metricGrpcMessageRequest  = "grpc_message_request"
	metricGrpcMessageResponse = "grpc_message_response"
	metricDuration            = "duration"
	metricDuration50          = "duration_50"
	metricDuration95          = "duration_95"
	metricDuration99          = "duration_99"
	metricRequestSize         = "request_size"
	metricRequestSize50       = "request_size_50"
	metricRequestSize95       = "request_size_95"
	metricRequestSize99       = "request_size_99"
	metricResponseSize        = "response_size"
	metricResponseSize50      = "response_size_50"
	metricResponseSize95      = "response_size_95"
	metricResponseSize99      = "response_size_99"
)

var (
	promURL = flag.String("promURL", "http://prometheus.istio-system:9090", "the URL of the Prometheus server -- http://localhost:9090")
)

func StartTrafficInformer(ctx context.Context, db *memory.Database) {
	defer runtime.HandleCrash()

	d, err := time.ParseDuration(duration)
	if err != nil {
		log.Warn("cannot parse %s into duration, fallback to 5m", duration)
		d = 5 * time.Minute
	}

	go func(ctx context.Context) {
		initTrafficTable(db)
		sqlCtx := sql.NewContext(ctx)
		queryMetrics(sqlCtx)

		trafficTable := table(TrafficTableName)
		if trafficTable == nil {
			log.Fatalf("table %s does not exist", TrafficTableName)
		}

		for {
			select {
			case <-time.After(d):
				trafficTable.Drop(sqlCtx)
				trafficTable.(*TrafficTable).table = createTrafficTable(db)
				queryMetrics(sqlCtx)
			case <-ctx.Done():
				return

			}
		}

	}(ctx)

}

func queryMetrics(ctx context.Context) {

	queries := []string{
		requestCountQuery,
		grpcMessageRequestQuery,
		grpcMessageResponseQuery,
		requestDurationQuery,
		requestDuration50Query,
		requestDuration95Query,
		requestDuration99Query,
		requestSizeQuery,
		requestSize50Query,
		requestSize95Query,
		requestSize99Query,
		responseSizeQuery,
		responseSize50Query,
		responseSize95Query,
		responseSize99Query,
	}

	metrics := []string{
		metricHttpRequest,
		metricGrpcMessageRequest,
		metricGrpcMessageResponse,
		metricDuration,
		metricDuration50,
		metricDuration95,
		metricDuration99,
		metricRequestSize,
		metricRequestSize50,
		metricRequestSize95,
		metricRequestSize99,
		metricResponseSize,
		metricResponseSize50,
		metricResponseSize95,
		metricResponseSize99,
	}

	streamPromResp := make(chan *promQueryResponse, len(queries))

	go func() {
		defer close(streamPromResp)

		var wg sync.WaitGroup
		wg.Add(len(queries))
		defer wg.Wait()

		for i, query := range queries {
			log.Debugf(">>> %s: %s\n", metrics[i], query)
			go func(metricName, query string) {
				defer wg.Done()
				u, err := url.Parse(fmt.Sprintf("%s/api/v1/query", *promURL))
				if err != nil {
					log.Fatal(err)
				}

				q := u.Query()
				q.Set("query", query)
				u.RawQuery = q.Encode()

				resp, err := http.Get(u.String())
				if err != nil {
					log.Fatal(err)
				}
				defer resp.Body.Close()

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					log.Fatal(err)
				}

				promResponse := promQueryResponse{MetricName: metricName}
				json.Unmarshal(body, &promResponse)

				streamPromResp <- &promResponse
				log.Debugf("<<< %s: %s\n", metricName, query)
			}(metrics[i], query)
		}

	}()

	go func(ctx context.Context) {
		trafficTable := table(TrafficTableName)
		if trafficTable == nil {
			log.Fatal("table %s does not exist", TrafficTableName)
		}
		for resp := range streamPromResp {
			ctx := sql.NewContext(ctx)
			err := trafficTable.Insert(ctx, resp)

			if err != nil {
				panic(err)
			}
		}
	}(ctx)

}

type promQueryResponse struct {
	MetricName string
	Status     string                `json:"status,omitempty"`
	Data       promQueryResponseData `json:"data,omitempty"`
}

type promQueryResponseData struct {
	ResultType string                    `json:"resultType,omitempty"`
	Result     []promQueryResponseResult `json:"result,omitempty"`
}

type promQueryResponseResult struct {
	Metric promQueryResultResultMetric `json:"metric,omitempty"`
	Value  []interface{}               `json:"value,omitempty"`
}

func (p *promQueryResponseResult) CastValue() float64 {
	v := p.Value[1]
	f, err := strconv.ParseFloat(v.(string), 64)
	if err != nil {
		log.Warn(err)
		return math.NaN()
	}
	return f
}

func (p *promQueryResponseResult) CastHTTPCode() int32 {
	v := p.Metric.ResponseCode
	if v == "" {
		return -1
	}

	i, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		log.Warn(err)
		return 0
	}
	return int32(i)
}

func (p *promQueryResponseResult) CastGRPCCode() int32 {
	v := p.Metric.GRPCResponseStatus
	if v == "" {
		return -1
	}

	i, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		log.Warn(err)
		return 0
	}
	return int32(i)
}

type promQueryResultResultMetric struct {
	App                             string `json:"app,omitempty"`
	ConnectionSecurityPolicy        string `json:"connection_security_policy,omitempty"`
	DestinationApp                  string `json:"destination_app,omitempty"`
	DestinationCanonicalRevision    string `json:"destination_canonical_revision,omitempty"`
	DestinationCanonicalService     string `json:"destination_canonical_service,omitempty"`
	DestinationCluster              string `json:"destination_cluster,omitempty"`
	DestinationPrincipal            string `json:"destination_principal,omitempty"`
	DestinationService              string `json:"destination_service,omitempty"`
	DestinationServiceName          string `json:"destination_service_name,omitempty"`
	DestinationServiceNamespace     string `json:"destination_service_namespace,omitempty"`
	DestinationVersion              string `json:"destination_version,omitempty"`
	DestinationWorkload             string `json:"destination_workload,omitempty"`
	DestinationWorkloadNamespace    string `json:"destination_workload_namespace,omitempty"`
	Instance                        string `json:"instance,omitempty"`
	Job                             string `json:"job,omitempty"`
	Namespace                       string `json:"namespace,omitempty"`
	Pod                             string `json:"pod,omitempty"`
	PodTemplateHash                 string `json:"pod_template_hash,omitempty"`
	Reporter                        string `json:"reporter,omitempty"`
	RequestProtocol                 string `json:"request_protocol,omitempty"`
	ResponseCode                    string `json:"response_code,omitempty"`
	GRPCResponseStatus              string `json:"grpc_response_status,omitempty"`
	ResponseFlags                   string `json:"response_flags,omitempty"`
	SecurityIstioIOTlsMode          string `json:"security_istio_io_tlsMode,omitempty"`
	ServiceIstioIOCanonicalName     string `json:"service_istio_io_canonical_name,omitempty"`
	ServiceIstioIOCanonicalRevision string `json:"service_istio_io_canonical_revision,omitempty"`
	SourceApp                       string `json:"source_app,omitempty"`
	SourceCanonicalRevision         string `json:"source_canonical_revision,omitempty"`
	SourceCanonicalService          string `json:"source_canonical_service,omitempty"`
	SourceCluster                   string `json:"source_cluster,omitempty"`
	SourcePrincipal                 string `json:"source_principal,omitempty"`
	SourceVersion                   string `json:"source_version,omitempty"`
	SourceWorkload                  string `json:"source_workload,omitempty"`
	SourceWorkloadNamespace         string `json:"source_workload_namespace,omitempty"`
	Version                         string `json:"version,omitempty"`
}

func (p *promQueryResponse) Rows() []sql.Row {
	rows := make([]sql.Row, len(p.Data.Result))
	for i, result := range p.Data.Result {
		rows[i] = sql.NewRow(
			result.Metric.SourceWorkload,
			result.Metric.SourceWorkloadNamespace,
			result.Metric.DestinationWorkload,
			result.Metric.Pod,
			result.Metric.Instance,
			result.Metric.DestinationServiceName,
			result.Metric.DestinationWorkloadNamespace,
			result.Metric.RequestProtocol,
			result.CastHTTPCode(),
			result.CastGRPCCode(),
			p.MetricName,
			result.CastValue())
	}
	return rows
}

type TrafficTable struct {
	db     *memory.Database
	table  *memory.Table
	logger *logrus.Entry
}

func initTrafficTable(db *memory.Database) {
	if _, ok := tables[TrafficTableName]; !ok {
		tables[TrafficTableName] = &TrafficTable{
			db:     db,
			table:  createTrafficTable(db),
			logger: tableLogger(PodMetricsTableName),
		}
	}
}

func createTrafficTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(TrafficTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "src_deployment", Type: sql.Text, Nullable: false, Source: TrafficTableName},
		{Name: "src_namespace", Type: sql.Text, Nullable: false, Source: TrafficTableName},
		{Name: "dst_deployment", Type: sql.Text, Nullable: false, Source: TrafficTableName},
		{Name: "dst_pod", Type: sql.Text, Nullable: false, Source: TrafficTableName},
		{Name: "dst_instance", Type: sql.Text, Nullable: false, Source: TrafficTableName},
		{Name: "dst_service", Type: sql.Text, Nullable: false, Source: TrafficTableName},
		{Name: "dst_namespace", Type: sql.Text, Nullable: false, Source: TrafficTableName},
		{Name: "protocol", Type: sql.Text, Nullable: false, Source: TrafficTableName},
		{Name: "http_status_code", Type: sql.Int32, Nullable: false, Source: TrafficTableName},
		{Name: "grpc_status_code", Type: sql.Int32, Nullable: false, Source: TrafficTableName},
		{Name: "metric", Type: sql.Text, Nullable: false, Source: TrafficTableName},
		{Name: "value", Type: sql.Float64, Nullable: false, Source: TrafficTableName},
	}), db.GetForeignKeyCollection())

	db.AddTable(TrafficTableName, table)
	log.Infof("table [%s] created", TrafficTableName)
	return table
}

func (t *TrafficTable) Log() *logrus.Entry {
	return t.logger
}

func (t *TrafficTable) Drop(ctx *sql.Context) error {
	if t == nil {
		return nil
	}
	return t.db.DropTable(ctx, TrafficTableName)
}
func (t *TrafficTable) Insert(ctx *sql.Context, resource interface{}) error {
	promResp, ok := resource.(*promQueryResponse)
	if !ok {
		return fmt.Errorf("resource is not of type *PromQueryResponse")
	}

	errCount := 0

	for _, row := range promResp.Rows() {
		err := t.table.Insert(ctx, row)
		t.Log().Debugf("inserting: [%s] ->[%s] -- error[%v]\n", row[0], row[3], err)
		if err != nil {
			t.Log().Warn(err)
			errCount++
		}
	}
	if errCount > 0 {
		return fmt.Errorf("%d errors when inserting row into %s", errCount, TrafficTableName)
	}
	return nil
}

func (t *TrafficTable) Delete(ctx *sql.Context, resource interface{}) error {
	t.Log().Warn("delete in table %s is not implemented", TrafficTableName)
	return nil
}

func (t *TrafficTable) Update(ctx *sql.Context, oldres, newres interface{}) error {
	t.Log().Warn("update table %s is not implemented", TrafficTableName)
	return nil
}
