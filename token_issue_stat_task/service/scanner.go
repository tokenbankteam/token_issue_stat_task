// Package service 先扫描 t_token_extension 中 issuer 非空的记录（第一段），
// 再按 token_id 批量取 t_token 的 holder_count / address / bl_symbol（第二段），
// 组装 issue / issue_time / holder_count / market_cap 后推送到 swap_token_search_task 的 topic。
package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"

	cmodel "github.com/tokenbankteam/tb_common/model"
	"github.com/tokenbankteam/token_issue_stat_task/config"
)

// extensionRow t_token_extension 的扫描结果（第一段 ✓）
type extensionRow struct {
	TokenId           int64  `gorm:"column:token_id"`
	Issuer            string `gorm:"column:issuer"`
	IssueTime         int64  `gorm:"column:issue_time"`
	CirculatingSupply string `gorm:"column:circulating_supply"`
}

// tokenRow t_token 的基础信息（第二段按 token_id 批量取 ✓）
type tokenRow struct {
	Id           int64  `gorm:"column:id"`
	BlockchainId int64  `gorm:"column:blockchain_id"`
	Address      string `gorm:"column:address"`
	BlSymbol     string `gorm:"column:bl_symbol"`
	HolderCount  int64  `gorm:"column:holder_count"`
	TotalSupply  string `gorm:"column:total_supply"`
}

// Scanner 扫描器
type Scanner struct {
	taskName   string
	cfg        *config.Config
	db         *gorm.DB
	writer     *kafka.Writer
	pricer     *priceClient
	blockChain *blockChainCache
	// bcWriter 香港集群 writer，仅启动时发送一次 earning_rw_blockchain
	bcWriter *kafka.Writer
}

func New(cfg *config.Config, db *gorm.DB) *Scanner {
	s := &Scanner{
		taskName:   "token-issuer-stat",
		cfg:        cfg,
		db:         db,
		pricer:     newPriceClient(cfg.TbServerAddr, cfg.TbServerTimeout()),
		blockChain: newBlockChainCache(cfg.TbServerAddr, cfg.TbServerTimeout()),
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      cfg.BrokerList,
			Topic:        cfg.TokenStatResultTopic,
			Balancer:     &kafka.Hash{},
			BatchSize:    200,
			BatchTimeout: 200 * time.Millisecond,
			RequiredAcks: int(kafka.RequireOne),
		}),
	}
	if len(cfg.HkAutoMqBrokerList) > 0 && cfg.EarningRwBlockchainTopic != "" {
		s.bcWriter = kafka.NewWriter(kafka.WriterConfig{
			Brokers:      cfg.HkAutoMqBrokerList,
			Topic:        cfg.EarningRwBlockchainTopic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: int(kafka.RequireOne),
		})
	}
	return s
}

func (s *Scanner) Close() error {
	if s.bcWriter != nil {
		if err := s.bcWriter.Close(); err != nil {
			log.Error().Err(err).Msg("close earning_rw_blockchain writer failed")
		}
	}
	return s.writer.Close()
}

// Run 扫一轮就结束（扫完打印成功日志后进程退出 ✓）
// 支持断点续扫：每批结束把 last_token_id 落到本地断点文件 ✓
func (s *Scanner) Run(ctx context.Context) error {
	log.Info().
		Int("batchSize", s.cfg.Scanner.BatchSize).
		Int("sleepMillis", s.cfg.Scanner.SleepMillis).
		Bool("resume", s.cfg.Scanner.Resume).
		Str("topic", s.cfg.TokenStatResultTopic).
		Msg("token-issuer-stat scanner start")

	// 启动时只从 tb_server 拉取一次链配置，后面全走内存缓存
	if _, err := s.blockChain.List(ctx); err != nil {
		log.Error().Err(err).Msg("load block chain once failed")
	}

	// 启动时把链维度数据发送一次到香港集群，之后只走本地缓存，不再发送。
	if err := s.sendBlockChainsOnce(ctx); err != nil {
		log.Error().Err(err).Msg("send block chain once failed")
	}

	// 断点：读取上次位置（本地文件 ✓）
	var (
		startFrom             int64
		baseScanned, baseSent int64
	)
	checkpointPath := s.cfg.Scanner.CheckpointFile
	if s.cfg.Scanner.Resume {
		cp, err := loadCheckpoint(checkpointPath)
		if err != nil {
			log.Warn().Err(err).Msg("load checkpoint failed, fallback to full scan")
		} else if cp != nil {
			startFrom = cp.LastTokenId
			baseScanned, baseSent = cp.Scanned, cp.Sent
			log.Info().
				Str("checkpoint", checkpointPath).
				Int64("lastTokenId", cp.LastTokenId).
				Int64("scanned", cp.Scanned).
				Int64("sent", cp.Sent).
				Msg("resume from checkpoint")
		}
	}
	if s.cfg.Scanner.Reset {
		if err := resetCheckpoint(checkpointPath); err != nil {
			log.Warn().Err(err).Msg("reset checkpoint failed")
		}
		startFrom = 0
	}

	startAt := time.Now()
	if err := s.scanFrom(ctx, startFrom, baseScanned, baseSent, checkpointPath); err != nil {
		log.Error().Err(err).Dur("cost", time.Since(startAt)).Msg("scan failed (checkpoint kept, rerun to resume)")
		return err
	}
	// 扫完删掉断点文件：下次运行从头开始 ✓
	if err := resetCheckpoint(checkpointPath); err != nil {
		log.Warn().Err(err).Msg("reset checkpoint after done failed")
	}
	log.Info().Dur("cost", time.Since(startAt)).Msg("scan success, exit")
	return nil
}

// scanOnce 第一段按 token_id 游标扫 t_token_extension（issuer 非空），
// 第二段按这批 token_id 取 t_token，然后推送。
func (s *Scanner) scanFrom(ctx context.Context, startFrom, baseScanned, baseSent int64, checkpointPath string) error {
	var (
		lastTokenId  = startFrom
		extensions   int
		msgsSent     int
		batches      int
		startAt      = time.Now()
		scannedTotal = baseScanned // 断点累计（跨重启累加 ✓）
		sentTotal    = baseSent
	)
	for {
		if ctx.Err() != nil {
			return nil
		}

		// ① 第一段：扫 t_token_extension（issuer 非空 ✓，走主键 token_id 游标 ✓）
		extRows, err := s.queryExtensionBatch(ctx, lastTokenId, s.cfg.Scanner.BatchSize)
		if err != nil {
			return err
		}
		if len(extRows) == 0 {
			log.Info().
				Int("extensions", extensions).
				Int("messages", msgsSent).
				Int("batches", batches).
				Dur("cost", time.Since(startAt)).
				Msg("scan round done")
			return nil
		}
		batches++
		extensions += len(extRows)
		scannedTotal += int64(len(extRows))
		lastTokenId = extRows[len(extRows)-1].TokenId

		// ② 第二段：按 token_id 批量取 t_token
		tokenMap, err := s.queryTokens(ctx, extRows)
		if err != nil {
			return err
		}

		// ③ 价格走 tb_server /v1/token/token_price_list（单次最多 100 个，内部自动分批 ✓）
		prices, err := s.queryPrices(ctx, extRows, tokenMap)
		if err != nil {
			return err
		}

		msgs := s.buildMessages(ctx, extRows, tokenMap, prices)
		if len(msgs) > 0 {
			if err := s.writer.WriteMessages(ctx, msgs...); err != nil {
				return err
			}
			msgsSent += len(msgs)
			sentTotal += int64(len(msgs))
		}
		// ★ 每批结束落断点（本地文件 ✓），断了能从这里继续 ✓
		if err := saveCheckpoint(checkpointPath, &checkpoint{
			LastTokenId: lastTokenId,
			Scanned:     scannedTotal,
			Sent:        sentTotal,
		}); err != nil {
			log.Warn().Err(err).Int64("lastTokenId", lastTokenId).Msg("save checkpoint failed")
		}
		//if batches%20 == 0 {
		log.Info().
			Int64("lastTokenId", lastTokenId).
			Int("extensions", extensions).
			Int("messages", msgsSent).
			Msg("scan progress")
		//}

		// 批间休眠，避免打死数据库
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Duration(s.cfg.Scanner.SleepMillis) * time.Millisecond):
		}
	}
}

// queryExtensionBatch 第一段：issuer 非空的 extension，按 token_id 游标分页
func (s *Scanner) queryExtensionBatch(ctx context.Context, lastTokenId int64, limit int) ([]*extensionRow, error) {
	rows := make([]*extensionRow, 0, limit)
	err := s.db.WithContext(ctx).Raw(`
SELECT token_id,
       issuer,
       issue_time,
       circulating_supply
FROM t_token_extension
WHERE issuer IS NOT NULL
  AND issuer <> ''
  AND token_id > ?
ORDER BY token_id
LIMIT ?`, lastTokenId, limit).Scan(&rows).Error
	return rows, err
}

// queryTokens 第二段：按 token_id 批量取 t_token 基础信息（单次 IN 查询 ✓）
func (s *Scanner) queryTokens(ctx context.Context, extRows []*extensionRow) (map[int64]*tokenRow, error) {
	result := make(map[int64]*tokenRow, len(extRows))
	if len(extRows) == 0 {
		return result, nil
	}
	tokenIds := make([]int64, 0, len(extRows))
	for _, ext := range extRows {
		if ext != nil && ext.TokenId > 0 {
			tokenIds = append(tokenIds, ext.TokenId)
		}
	}
	if len(tokenIds) == 0 {
		return result, nil
	}
	tokens := make([]*tokenRow, 0, len(tokenIds))
	err := s.db.WithContext(ctx).Raw(`
SELECT id,
       blockchain_id,
       address,
       bl_symbol,
       holder_count
FROM t_token
WHERE id IN (?)`, tokenIds).Scan(&tokens).Error
	if err != nil {
		return nil, err
	}
	for _, token := range tokens {
		if token == nil {
			continue
		}
		result[token.Id] = token
	}
	return result, nil
}

// queryPrices 按本批 token 调 tb_server 拿价格，返回 map[tokenId]price
func (s *Scanner) queryPrices(ctx context.Context, extRows []*extensionRow, tokenMap map[int64]*tokenRow) (map[int64]float64, error) {
	result := make(map[int64]float64, len(extRows))
	keys := make([]*cmodel.TokenKey, 0, len(extRows))
	ids := make([]int64, 0, len(extRows))
	for _, ext := range extRows {
		if ext == nil {
			continue
		}
		token := tokenMap[ext.TokenId]
		if token == nil || token.Address == "" {
			continue
		}
		keys = append(keys, &cmodel.TokenKey{
			BlockchainId: token.BlockchainId,
			Address:      s.canonicalAddress(ctx, token),
			BlSymbol:     token.BlSymbol,
		})
		ids = append(ids, ext.TokenId)
	}
	if len(keys) == 0 {
		return result, nil
	}
	prices, err := s.pricer.GetPrices(ctx, keys)
	if err != nil {
		return nil, err
	}
	for i, id := range ids {
		if i < len(prices) {
			result[id] = prices[i]
		}
	}
	return result, nil
}

// canonicalAddress 按链的 namespace 规整 token 地址（EVM 系链转小写，其它链原样）。
// namespace 来自 tb_server /v1/block_chain/list 的本地缓存；取不到时退回按链 id 判断。
func (s *Scanner) canonicalAddress(ctx context.Context, token *tokenRow) string {
	if token == nil {
		return ""
	}
	namespace := s.blockChain.GetNamespace(ctx, token.BlockchainId)
	return checkAddressAndToLower(token.Address, token.BlockchainId, namespace)
}

// buildMessages 组装 TokenStatTaskMsg（issue / issue_time / holder_count / market_cap）
func (s *Scanner) buildMessages(ctx context.Context, extRows []*extensionRow, tokenMap map[int64]*tokenRow, prices map[int64]float64) []kafka.Message {
	msgs := make([]kafka.Message, 0, len(extRows))
	for _, ext := range extRows {
		if ext == nil || ext.TokenId <= 0 {
			continue
		}
		token := tokenMap[ext.TokenId]
		if token == nil || token.Address == "" {
			continue // t_token 里取不到（已删除 / id 不一致）→ 跳过 ✓
		}
		// ★ EVM 系链（namespace=ETHEREUM，或 eth/heco/bsc/okc/klaytn）地址统一转小写，
		// 消息体和 Kafka key 都用转换后的地址，避免大小写不一致导致下游把同一个 token 当两条。
		namespace := s.blockChain.GetNamespace(ctx, token.BlockchainId)
		address := checkAddressAndToLower(token.Address, token.BlockchainId, namespace)
		// issuer 也是链上地址，与 token 地址同一套规整规则，避免大小写混用
		issuer := checkAddressAndToLower(ext.Issuer, token.BlockchainId, namespace)
		tokenKey := &cmodel.TokenKey{
			BlockchainId: token.BlockchainId,
			Address:      address,
			BlSymbol:     token.BlSymbol,
		}
		attribute := &cmodel.TokenAttribute{
			Issuer:    issuer,
			IssueTime: ext.IssueTime,
			MarketCap: calcMarketCap(token, ext, prices[ext.TokenId]),
		}
		// holder_count 为 0 表示本次没取到，保持零值不覆盖下游已有数据
		if token.HolderCount > 0 {
			attribute.HolderCount = token.HolderCount
		}
		msg := &cmodel.TokenStatTaskMsg{
			TokenKey:       tokenKey,
			TokenAttribute: attribute,
		}
		value, err := json.Marshal(msg)
		if err != nil {
			log.Error().Err(err).Int64("tokenId", ext.TokenId).Msg("marshal message failed")
			continue
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(tokenKey.UniqueKey()),
			Value: value,
		})
	}
	return msgs
}

// calcMarketCap 市值 = 流通量(circulating_supply) × 价格
// 流通量缺失 / 为 0 / 非法 / 负数 → 返回 0（消息里不带 market_cap ✓ 不会把 ES 刷成 0 ✓）
func calcMarketCap(token *tokenRow, ext *extensionRow, priceUsd float64) float64 {
	if token == nil || priceUsd <= 0 || ext == nil {
		return 0
	}
	circulatingSupply := ext.CirculatingSupply
	if circulatingSupply == "" || circulatingSupply == "0" {
		return 0
	}
	circulating, err := decimal.NewFromString(circulatingSupply)
	if err != nil || circulating.IsNegative() || circulating.IsZero() {
		return 0
	}
	marketCap, _ := circulating.Mul(decimal.NewFromFloat(priceUsd)).Truncate(4).Float64()
	return marketCap
}
