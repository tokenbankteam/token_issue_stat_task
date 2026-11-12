package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	cmodel "github.com/tokenbankteam/tb_common/model"
)

// blockChainListResponse tb_server /v1/block_chain/listnocache 的返回结构
type blockChainListResponse struct {
	Result  int64                `json:"result"`
	Message string               `json:"message"`
	Data    []*cmodel.BlockChain `json:"data"`
}

// blockChainCache tb_server 链配置的本地缓存。
// 进程启动后只从 tb_server 拉取一次（sync.Once），之后全部走内存，不再刷新。
// 主要用途：拿到 namespace，配合链 id 决定地址是否需要转小写。
type blockChainCache struct {
	baseURL    string
	httpClient *http.Client

	once sync.Once

	mu   sync.RWMutex
	byId map[int64]*cmodel.BlockChain
	err  error
}

func newBlockChainCache(baseURL string, timeout time.Duration) *blockChainCache {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &blockChainCache{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: timeout},
		byId:       make(map[int64]*cmodel.BlockChain),
	}
}

// ensureLoaded 保证链配置只加载一次；加载失败也不重试。
func (c *blockChainCache) ensureLoaded(ctx context.Context) {
	c.once.Do(func() {
		c.err = c.load(ctx)
		if c.err != nil {
			log.Error().Err(c.err).Msg("load block chain from tb_server failed")
		}
	})
}

// Get 返回指定链的配置；未加载成功时返回 nil（调用方按链 id 兜底）。
func (c *blockChainCache) Get(ctx context.Context, blockChainId int64) *cmodel.BlockChain {
	c.ensureLoaded(ctx)
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.byId[blockChainId]
}

// GetNamespace 返回链的 namespace；取不到时返回空串。
func (c *blockChainCache) GetNamespace(ctx context.Context, blockChainId int64) string {
	blockChain := c.Get(ctx, blockChainId)
	if blockChain == nil {
		return ""
	}
	return blockChain.GetNamespace()
}

// List 返回全部链配置；启动时加载失败则返回错误。
func (c *blockChainCache) List(ctx context.Context) ([]*cmodel.BlockChain, error) {
	c.ensureLoaded(ctx)
	if c.err != nil {
		return nil, c.err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.byId) == 0 {
		return nil, fmt.Errorf("block chain cache is empty")
	}
	list := make([]*cmodel.BlockChain, 0, len(c.byId))
	for _, blockChain := range c.byId {
		list = append(list, blockChain)
	}
	return list, nil
}

// load 分页拉全量链配置并写入缓存。
func (c *blockChainCache) load(ctx context.Context) error {
	const (
		pageSize = 100
		maxPages = 50
	)
	all := make([]*cmodel.BlockChain, 0, pageSize)
	for page := 0; page < maxPages; page++ {
		batch, err := c.fetchPage(ctx, int64(page*pageSize), pageSize)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		all = append(all, batch...)
		if len(batch) < pageSize {
			break
		}
	}
	if len(all) == 0 {
		return fmt.Errorf("block chain list is empty")
	}

	byId := make(map[int64]*cmodel.BlockChain, len(all))
	for _, blockChain := range all {
		if blockChain == nil || blockChain.Id <= 0 {
			continue
		}
		byId[blockChain.Id] = blockChain
	}

	c.mu.Lock()
	c.byId = byId
	c.mu.Unlock()

	log.Info().Int("chains", len(byId)).Msg("block chain loaded once from tb_server")
	return nil
}

func (c *blockChainCache) fetchPage(ctx context.Context, start int64, count int) ([]*cmodel.BlockChain, error) {
	url := fmt.Sprintf("%s/v1/block_chain/listnocache?start=%d&count=%d", c.baseURL, start, count)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call block_chain list: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("block_chain list http %d: %s", resp.StatusCode, string(raw))
	}
	parsed := &blockChainListResponse{}
	if err := json.Unmarshal(raw, parsed); err != nil {
		return nil, fmt.Errorf("decode block_chain list: %w", err)
	}
	if parsed.Result != 0 {
		return nil, fmt.Errorf("block_chain list result=%d msg=%s", parsed.Result, parsed.Message)
	}
	return parsed.Data, nil
}
