package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

func main() {
	// 定义命令行参数
	dsn := flag.String("dsn", "", "PostgreSQL DSN")
	pushURL := flag.String("push-url", "", "Pushgateway URL")
	interval := flag.Duration("interval", time.Minute, "Interval between queries")
	jobName := flag.String("job", "", "job name for Pushgateway")
	env := flag.String("env", "", "Environment: dev/prod")
	rpcURL := flag.String("rpc-url", "", "RPC URL to get block number")
	hostname := flag.String("hostname", "", "Hostname of the server")

	flag.Parse()

	if *dsn == "" || *pushURL == "" || *rpcURL == "" {
		log.Fatal("DSN, push-url, and rpc-url must be provided")
	}

	log.Printf("Starting program with DSN: %s, Pushgateway URL: %s, Interval: %s, Job Name: %s, RPC URL: %s", *dsn, *pushURL, *interval, *jobName, *rpcURL)

	// 连接到数据库
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()
	log.Println("Successfully connected to the database")

	// 定义 Prometheus Gauge
	blockHeightGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "quai_block_height",
		Help: "The block height of quai from different sources",
	})

	// 定期查询数据库和RPC并推送数据到 Pushgateway
	for {
		log.Println("Querying the database for the maximum block height...")
		var maxHeight int
		err := db.QueryRow("SELECT MAX(number) FROM blocks").Scan(&maxHeight)
		if err != nil {
			log.Printf("Failed to execute query: %v", err)
			blockHeightGauge.Set(-1) // 查询失败时，设置为特殊值
			pushMetrics(*pushURL, *jobName, *env, "db", "", blockHeightGauge)
		} else {
			log.Printf("Query successful, maximum block height: %d", maxHeight)
			blockHeightGauge.Set(float64(maxHeight))
			pushMetrics(*pushURL, *jobName, *env, "db", "", blockHeightGauge)
		}

		log.Println("Querying the RPC for the current block height...")
		rpcHeight, err := fetchBlockHeightFromRPC(*rpcURL)
		if err != nil {
			log.Printf("Failed to fetch block height from RPC: %v", err)
			blockHeightGauge.Set(-1) // 查询失败时，设置为特殊值
			pushMetrics(*pushURL, *jobName, *env, "rpc", *hostname, blockHeightGauge)
		} else {
			log.Printf("RPC query successful, current block height: %d", rpcHeight)
			blockHeightGauge.Set(float64(rpcHeight))
			pushMetrics(*pushURL, *jobName, *env, "rpc", *hostname, blockHeightGauge)
		}

		log.Printf("Sleeping for %s before the next query", *interval)
		time.Sleep(*interval)
	}
}

// fetchBlockHeightFromRPC 从 RPC 获取块高度
func fetchBlockHeightFromRPC(rpcURL string) (int64, error) {
	payload := `{
		"jsonrpc": "2.0",
		"method": "quai_blockNumber",
		"params": [],
		"id": 1
	}`

	req, err := http.NewRequest("POST", rpcURL, strings.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %w", err)
	}

	var rpcResponse struct {
		Result string `json:"result"`
	}

	if err := json.Unmarshal(body, &rpcResponse); err != nil {
		return 0, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// 将 16 进制的 result 转换为 10 进制的整数
	height, err := strconv.ParseInt(rpcResponse.Result, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse block height: %w", err)
	}

	return height, nil
}

// pushMetrics 使用 Pushgateway 推送数据
func pushMetrics(pushURL, jobName, env, source, hostname string, gauge prometheus.Gauge) {
	log.Printf("Pushing metrics to Pushgateway at %s with job name %s, source %s...", pushURL, jobName, source)

	// 创建 Pushgateway pusher 对象
	pusher := push.New(pushURL, jobName).
		Grouping("env", env).
		Grouping("source", source).
		Collector(gauge)

	// 如果提供了 hostname，则添加 hostname 标签
	if hostname != "" {
		pusher.Grouping("hostname", hostname)
	}

	// 推送数据
	if err := pusher.Push(); err != nil {
		log.Printf("Could not push to Pushgateway: %v", err)
	} else {
		log.Println("Pushed metrics successfully")
	}
}
