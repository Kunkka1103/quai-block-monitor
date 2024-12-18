package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

// 全局 HTTP 客户端，不设置超时，超时通过 context 控制
var httpClient = &http.Client{}

// fetchBlockHeightFromRPC 从 RPC 获取块高度，使用 context 设置超时
func fetchBlockHeightFromRPC(rpcURL string, timeout time.Duration) (int64, error) {
	payload := `{
        "jsonrpc": "2.0",
        "method": "quai_blockNumber",
        "params": [],
        "id": 1
    }`

	// 创建带超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", rpcURL, strings.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
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

	height, err := strconv.ParseInt(rpcResponse.Result, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse block height: %w", err)
	}
	return height, nil
}

// pushMetrics 使用 Pushgateway 推送数据
func pushMetrics(pushURL, jobName, env, source, hostname string, gauge prometheus.Gauge) {
	log.Printf("Pushing metrics to Pushgateway at %s with job name %s, source %s...", pushURL, jobName, source)
	pusher := push.New(pushURL, jobName).
		Grouping("env", env).
		Grouping("source", source)
	if hostname != "" {
		pusher = pusher.Grouping("hostname", hostname)
	}
	pusher.Collector(gauge)
	if err := pusher.Push(); err != nil {
		log.Printf("Could not push to Pushgateway: %v", err)
	} else {
		log.Println("Pushed metrics successfully")
	}
}

// processRPCNodes 并发处理 RPC 节点
func processRPCNodes(env string, rpcEntries []string, pushURL, jobName, envLabel string, rpcTimeout time.Duration) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // 控制并发数，例如最多10个并发请求

	for _, rpcEntry := range rpcEntries {
		wg.Add(1)
		go func(rpcEntry string) {
			defer wg.Done()
			sem <- struct{}{} // 获取一个并发槽
			defer func() { <-sem }() // 释放并发槽

			splitEntry := strings.Split(strings.TrimSpace(rpcEntry), ";")
			if len(splitEntry) != 2 {
				log.Printf("[%s] Invalid RPC entry format: %s", env, rpcEntry)
				return
			}
			rpcURL := splitEntry[0]
			hostname := splitEntry[1]

			rpcHeight, err := fetchBlockHeightFromRPC(rpcURL, rpcTimeout)
			if err != nil {
				log.Printf("[%s] Failed to query RPC (%s): %v", env, rpcURL, err)
				rpcHeight = -1
			} else {
				log.Printf("[%s] Block height from RPC (%s): %d", env, rpcURL, rpcHeight)
			}

			// 推送 RPC 节点的高度
			blockHeightGauge := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "quai_block_height",
				Help: "The block height of quai from different sources",
			})
			blockHeightGauge.Set(float64(rpcHeight))
			pushMetrics(pushURL, jobName, envLabel, "rpc", hostname, blockHeightGauge)
		}(rpcEntry)
	}

	wg.Wait()
}

func main() {
	// 命令行参数
	pushURL := flag.String("push-url", "", "Pushgateway URL")
	interval := flag.Duration("interval", time.Minute, "Interval between queries")
	jobName := flag.String("job", "", "Job name for Pushgateway")

	// dev 环境参数
	devDsn := flag.String("dev-dsn", "", "PostgreSQL DSN for dev environment")
	devRpcUrls := flag.String("dev-rpc-urls", "", "Comma-separated list of RPC URLs for dev environment, with hostname (url;hostname)")

	// prod 环境参数
	prodDsn := flag.String("prod-dsn", "", "PostgreSQL DSN for prod environment")
	prodRpcUrls := flag.String("prod-rpc-urls", "", "Comma-separated list of RPC URLs for prod environment, with hostname (url;hostname)")

	// 官方 URL
	officialURL := flag.String("official-url", "", "Official URL of the block height")

	flag.Parse()

	if *pushURL == "" || *officialURL == "" {
		log.Fatal("Pushgateway URL and official URL must be provided")
	}

	// dev 和 prod 环境的节点列表
	devNodeURLs := strings.Split(*devRpcUrls, ",")
	prodNodeURLs := strings.Split(*prodRpcUrls, ",")

	if *devDsn == "" && *prodDsn == "" {
		log.Fatal("At least one of dev-dsn or prod-dsn must be provided")
	}

	// 连接数据库
	var devDb, prodDb *sql.DB
	var err error

	if *devDsn != "" {
		devDb, err = sql.Open("postgres", *devDsn)
		if err != nil {
			log.Fatalf("[dev] Failed to connect to database: %v", err)
		}
		defer devDb.Close()
	}

	if *prodDsn != "" {
		prodDb, err = sql.Open("postgres", *prodDsn)
		if err != nil {
			log.Fatalf("[prod] Failed to connect to database: %v", err)
		}
		defer prodDb.Close()
	}

	// 定义一个全局的 RPC 请求超时时间，例如 10 秒
	const rpcTimeout = 10 * time.Second

	for {
		// 处理 dev 环境
		if devDb != nil {
			log.Println("Processing dev environment")

			// 查询数据库块高度
			var maxHeight int
			err = devDb.QueryRow("SELECT MAX(number) FROM blocks").Scan(&maxHeight)
			if err != nil {
				log.Printf("[dev] Failed to query database: %v", err)
				maxHeight = -1
			} else {
				log.Printf("[dev] Max block height from database: %d", maxHeight)
			}

			// 推送数据库高度
			blockHeightGauge := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "quai_block_height",
				Help: "The block height of quai from different sources",
			})
			blockHeightGauge.Set(float64(maxHeight))
			pushMetrics(*pushURL, *jobName, "dev", "db", "", blockHeightGauge)

			// 并发查询每个 RPC 节点的高度
			processRPCNodes("dev", devNodeURLs, *pushURL, *jobName, "dev", rpcTimeout)
		}

		// 处理 prod 环境
		if prodDb != nil {
			log.Println("Processing prod environment")

			// 查询数据库块高度
			var maxHeight int
			err = prodDb.QueryRow("SELECT MAX(number) FROM blocks").Scan(&maxHeight)
			if err != nil {
				log.Printf("[prod] Failed to query database: %v", err)
				maxHeight = -1
			} else {
				log.Printf("[prod] Max block height from database: %d", maxHeight)
			}

			// 推送数据库高度
			blockHeightGauge := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "quai_block_height",
				Help: "The block height of quai from different sources",
			})
			blockHeightGauge.Set(float64(maxHeight))
			pushMetrics(*pushURL, *jobName, "prod", "db", "", blockHeightGauge)

			// 并发查询每个 RPC 节点的高度
			processRPCNodes("prod", prodNodeURLs, *pushURL, *jobName, "prod", rpcTimeout)
		}

		// 查询官方 URL 的块高度
		officialHeight, err := fetchBlockHeightFromRPC(*officialURL, rpcTimeout)
		if err != nil {
			log.Printf("Failed to query official URL: %v", err)
			officialHeight = -1
		} else {
			log.Printf("Block height from official URL: %d", officialHeight)
		}

		// 推送官方 URL 的高度
		officialGauge := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "quai_block_height",
			Help: "The block height of quai from different sources",
		})
		officialGauge.Set(float64(officialHeight))
		pushMetrics(*pushURL, *jobName, "prod", *officialURL, "official", officialGauge)

		log.Printf("Sleeping for %s before the next query", *interval)
		time.Sleep(*interval)
	}
}
