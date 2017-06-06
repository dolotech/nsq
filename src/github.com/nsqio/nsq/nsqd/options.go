package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"
)

type Options struct {
	// basic options
	ID                       int64  `flag:"node-id" cfg:"id"` //进程的唯一码(默认是主机名的哈希值)
	LogLevel                 string `flag:"log-level"`
	logLevel                 int
	LogPrefix                string        `flag:"log-prefix"`
	Verbose                  bool          `flag:"verbose"` // for backwards compatibility 打开日志
	TCPAddress               string        `flag:"tcp-address"`
	HTTPAddress              string        `flag:"http-address"`                                       //为 HTTP 客户端监听 <addr>:<port>
	HTTPSAddress             string        `flag:"https-address"`                                      //为 HTTPS 客户端 监听 <addr>:<port>
	BroadcastAddress         string        `flag:"broadcast-address"`                                  //通过 lookupd  注册的地址（默认名是 OS）
	NSQLookupdTCPAddresses   []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"` //解析 TCP 地址名字 (可能会给多次)
	AuthHTTPAddresses        []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`        //<addr>:<port> 查询授权服务器 (可能会给多次)
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout"`
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"`

	// diskqueue options
	DataPath        string        `flag:"data-path"` //缓存消息的磁盘路径
	MemQueueSize    int64         `flag:"mem-queue-size"`
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"` //每个磁盘队列文件的字节数
	SyncEvery       int64         `flag:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	QueueScanInterval        time.Duration
	QueueScanRefreshInterval time.Duration
	QueueScanSelectionCount  int
	QueueScanWorkerPoolMax   int
	QueueScanDirtyPercent    float64

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout"`     //自动重新队列消息前需要等待的时间
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"` //消息超时的最大时间间隔
	MaxMsgSize    int64         `flag:"max-msg-size"`    //单个消息体的最大字节数
	MaxBodySize   int64         `flag:"max-body-size"`   // 单个命令体的最大尺寸
	MaxReqTimeout time.Duration `flag:"max-req-timeout"` //消息重新排队的超时时间
	ClientTimeout time.Duration

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`    // 在客户端心跳间，最大的客户端配置时间间隔
	MaxRdyCount            int64         `flag:"max-rdy-count"`             //客户端最大的 RDY 数量
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`    //最大客户端输出缓存可配置大小(字节）
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"` // 最大客户端输出缓存可配置大小(字节）

	// statsd integration
	StatsdAddress  string        `flag:"statsd-address"`   //统计进程的 UDP <addr>:<port
	StatsdPrefix   string        `flag:"statsd-prefix"`    //"nsq.%s": 发送给统计keys 的前缀(%s for host replacement)
	StatsdInterval time.Duration `flag:"statsd-interval"`  // 从推送到统计的时间间隔
	StatsdMemStats bool          `flag:"statsd-mem-stats"` //切换发送内存和 GC 统计数据

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`                                         //计算这段时间里，点对点时间延迟（例如，60s 仅计算过去 60 秒）
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"` //消息处理时间的百分比（通过逗号可以多次指定，默认为 none）

	// TLS config
	TLSCert             string `flag:"tls-cert"`               //证书文件路径
	TLSKey              string `flag:"tls-key"`                //私钥路径文件
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"` //客户端证书授权策略 ('require' or 'require-verify')
	TLSRootCAFile       string `flag:"tls-root-ca-file"`       // 私钥证书授权 PEM 路径
	TLSRequired         int    `flag:"tls-required"`           //客户端连接需求 TLS
	TLSMinVersion       uint16 `flag:"tls-min-version"`

	// compression
	DeflateEnabled  bool `flag:"deflate"`           //运行协商压缩特性（客户端压缩）
	MaxDeflateLevel int  `flag:"max-deflate-level"` //最大的压缩比率等级（> values == > nsqd CPU usage)
	SnappyEnabled   bool `flag:"snappy"`            //打开快速选项 (客户端压缩)

	Logger Logger
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  "info",
		logLevel:  2,

		TCPAddress:       "0.0.0.0:4150",
		HTTPAddress:      "0.0.0.0:4151",
		HTTPSAddress:     "0.0.0.0:4152",
		BroadcastAddress: hostname,

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 1 * time.Second,

		StatsdPrefix:   "nsq.%s",
		StatsdInterval: 60 * time.Second,
		StatsdMemStats: true,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}
}
