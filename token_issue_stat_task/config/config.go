package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 服务配置
type Config struct {
	Log     LogConfig     `yaml:"log"`
	DB      DBConfig      `yaml:"db"`
	Scanner ScannerConfig `yaml:"scanner"`

	// BrokerList kafka 集群（与 swap_token_search_task 相同 ✓）
	BrokerList []string `yaml:"brokerlist"`
	// TokenStatResultTopic swap_token_search_task 消费的 topic ✓
	TokenStatResultTopic string `yaml:"tokenstatresulttopic"`
	// TbServerAddr tb_server 地址（价格接口 /v1/token/token_price_list ✓）
	TbServerAddr string `yaml:"tbserveraddr"`
	// TbServerTimeoutSeconds 价格接口超时，默认 5s ✓
	TbServerTimeoutSeconds int `yaml:"tbservertimeoutseconds"`
	// HkAutoMqBrokerList 香港集群 automq（earning_rw_blockchain 用）
	HkAutoMqBrokerList []string `yaml:"hkautomqbrokerlist"`
	// EarningRwBlockchainTopic 链维度数据同步 topic，默认 earning_rw_blockchain
	EarningRwBlockchainTopic string `yaml:"earningrwblockchaintopic"`
}

// TbServerTimeout 价格接口超时
func (c *Config) TbServerTimeout() time.Duration {
	if c.TbServerTimeoutSeconds <= 0 {
		return 5 * time.Second
	}
	return time.Duration(c.TbServerTimeoutSeconds) * time.Second
}

type LogConfig struct {
	Level string `yaml:"level"`
}

type DBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	// 连接池：扫描任务只需要很小的池子，避免和其它服务抢连接
	MaxOpenConns int `yaml:"maxopenconns"`
	MaxIdleConns int `yaml:"maxidleconns"`
}

func (c DBConfig) DSN() string {
	port := c.Port
	if port == 0 {
		port = 3306
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		c.User, c.Password, c.Host, port, c.Database)
}

type ScannerConfig struct {
	// Enabled 便于临时关闭
	Enabled bool `yaml:"enabled"`
	// BatchSize 每批 token 数，默认 200
	BatchSize int `yaml:"batchsize"`
	// SleepMillis 每批处理完休眠毫秒数，默认 500
	SleepMillis int `yaml:"sleepmillis"`
	// Resume 断点续扫：为 true 时从上次记录的 last_token_id 继续（默认 true ✓）
	Resume bool `yaml:"resume"`
	// Reset 启动前清空断点，从头扫（一次性重跑用 ✓）
	Reset bool `yaml:"reset"`
	// CheckpointFile 断点文件路径（默认 ./checkpoint.json ✓）
	CheckpointFile string `yaml:"checkpointfile"`
}

// Load 读取 yaml 配置并填默认值
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if cfg.Scanner.BatchSize <= 0 {
		cfg.Scanner.BatchSize = 200
	}
	if cfg.Scanner.SleepMillis <= 0 {
		cfg.Scanner.SleepMillis = 500
	}
	if cfg.Scanner.CheckpointFile == "" {
		cfg.Scanner.CheckpointFile = "./checkpoint.json"
	}
	if cfg.TbServerAddr == "" {
		cfg.TbServerAddr = "http://127.0.0.1:8080"
	}
	if cfg.TbServerTimeoutSeconds <= 0 {
		cfg.TbServerTimeoutSeconds = 5
	}
	if cfg.EarningRwBlockchainTopic == "" {
		cfg.EarningRwBlockchainTopic = "earning_rw_blockchain"
	}
	if cfg.DB.MaxOpenConns <= 0 {
		cfg.DB.MaxOpenConns = 4
	}
	if cfg.DB.MaxIdleConns <= 0 {
		cfg.DB.MaxIdleConns = 2
	}
	return cfg, nil
}
