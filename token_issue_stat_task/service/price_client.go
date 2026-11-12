package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	cmodel "github.com/tokenbankteam/tb_common/model"
)

// priceKey /v1/token/token_price_list 的请求元素（字段名与 tb_server 的 TokenCommonKey 对齐）
type priceKey struct {
	BlockchainId int64  `json:"blockchain_id"`
	Address      string `json:"address"`
	BlSymbol     string `json:"bl_symbol"`
}

type priceListResponse struct {
	Result  int      `json:"result"`
	Message string   `json:"message"`
	Data    []string `json:"data"`
}

// priceClient 调 tb_server 的 /v1/token/token_price_list 拿价格
// 注意：该接口单次最多 100 个 token ✗，这里自动按 100 分批 ✓
type priceClient struct {
	baseURL string
	client  *http.Client
}

func newPriceClient(baseURL string, timeout time.Duration) *priceClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &priceClient{baseURL: baseURL, client: &http.Client{Timeout: timeout}}
}

// GetPrices 返回与入参一一对应的价格（拿不到的为 0 ✓）
func (c *priceClient) GetPrices(ctx context.Context, keys []*cmodel.TokenKey) ([]float64, error) {
	prices := make([]float64, len(keys))
	if len(keys) == 0 {
		return prices, nil
	}
	const maxBatch = 100 // ★ 接口限制：tokenLen <= 100 ✓
	for start := 0; start < len(keys); start += maxBatch {
		end := start + maxBatch
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[start:end]
		batchPrices, err := c.getPricesBatch(ctx, batch)
		if err != nil {
			return nil, err
		}
		copy(prices[start:end], batchPrices)
	}
	return prices, nil
}

func (c *priceClient) getPricesBatch(ctx context.Context, keys []*cmodel.TokenKey) ([]float64, error) {
	reqItems := make([]priceKey, 0, len(keys))
	for _, key := range keys {
		if key == nil {
			reqItems = append(reqItems, priceKey{})
			continue
		}
		reqItems = append(reqItems, priceKey{
			BlockchainId: key.BlockchainId,
			Address:      key.Address,
			BlSymbol:     key.BlSymbol,
		})
	}
	body, err := json.Marshal(reqItems)
	if err != nil {
		return nil, err
	}
	url := c.baseURL + "/v1/token/token_price_list"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call token_price_list: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token_price_list http %d: %s", resp.StatusCode, string(raw))
	}
	parsed := &priceListResponse{}
	if err := json.Unmarshal(raw, parsed); err != nil {
		return nil, fmt.Errorf("decode token_price_list: %w", err)
	}
	if parsed.Result != 0 {
		return nil, fmt.Errorf("token_price_list result=%d msg=%s", parsed.Result, parsed.Message)
	}
	prices := make([]float64, len(keys))
	for i := range keys {
		if i >= len(parsed.Data) {
			break
		}
		if v, err := strconv.ParseFloat(parsed.Data[i], 64); err == nil && v > 0 {
			prices[i] = v
		}
	}
	if len(parsed.Data) != len(keys) {
		log.Warn().Int("want", len(keys)).Int("got", len(parsed.Data)).Msg("token_price_list size mismatch")
	}
	return prices, nil
}
