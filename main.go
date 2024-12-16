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
		Grouping("source", source).
		Grouping("env", env).
		Collector(gauge)
	if hostname != "" {
		pusher.Grouping("hostname", hostname)
	}
	if err := pusher.Push(); err != nil {
		log.Printf("Could not push to Pushgateway: %v", err)
	} else {
		log.Println("Pushed metrics successfully")
	}
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

			// 查询每个 RPC 节点的高度
			for _, rpcEntry := range devNodeURLs {
				splitEntry := strings.Split(strings.TrimSpace(rpcEntry), ";")
				if len(splitEntry) != 2 {
					log.Printf("[dev] Invalid RPC entry format: %s", rpcEntry)
					continue
				}
				rpcURL := splitEntry[0]
				hostname := splitEntry[1]

				rpcHeight, err := fetchBlockHeightFromRPC(rpcURL)
				if err != nil {
					log.Printf("[dev] Failed to query RPC (%s): %v", rpcURL, err)
					rpcHeight = -1
				} else {
					log.Printf("[dev] Block height from RPC (%s): %d", rpcURL, rpcHeight)
				}

				// 推送 RPC 节点的高度
				blockHeightGauge.Set(float64(rpcHeight))
				pushMetrics(*pushURL, *jobName, "dev", "rpc", hostname, blockHeightGauge)
			}
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

			// 查询每个 RPC 节点的高度
			for _, rpcEntry := range prodNodeURLs {
				splitEntry := strings.Split(strings.TrimSpace(rpcEntry), ";")
				if len(splitEntry) != 2 {
					log.Printf("[prod] Invalid RPC entry format: %s", rpcEntry)
					continue
				}
				rpcURL := splitEntry[0]
				hostname := splitEntry[1]

				rpcHeight, err := fetchBlockHeightFromRPC(rpcURL)
				if err != nil {
					log.Printf("[prod] Failed to query RPC (%s): %v", rpcURL, err)
					rpcHeight = -1
				} else {
					log.Printf("[prod] Block height from RPC (%s): %d", rpcURL, rpcHeight)
				}

				// 推送 RPC 节点的高度
				blockHeightGauge.Set(float64(rpcHeight))
				pushMetrics(*pushURL, *jobName, "prod", "rpc", hostname, blockHeightGauge)
			}
		}

		// 查询官方 URL 的块高度
		officialHeight, err := fetchBlockHeightFromRPC(*officialURL)
		if err != nil {
			log.Printf("Failed to query official URL: %v", err)
			officialHeight = -1
		} else {
			log.Printf("Block height from official URL: %d", officialHeight)
		}

		// 推送官方 URL 的高度
		blockHeightGauge := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "quai_block_height",
			Help: "The block height of quai from different sources",
		})
		blockHeightGauge.Set(float64(officialHeight))
		pushMetrics(*pushURL, *jobName, "prod", *officialURL, "official",blockHeightGauge)

		log.Printf("Sleeping for %s before the next query", *interval)
		time.Sleep(*interval)
	}
}
