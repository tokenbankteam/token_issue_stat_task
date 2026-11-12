package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	cmodel "github.com/tokenbankteam/tb_common/model"
)

// earningRwBlockChainMsg 同步到 risingwave（blockchain_info）的链维度数据。
// value_proportion 由消费端 sink 固定写入，生产端不发。
type earningRwBlockChainMsg struct {
	BlockChainId int64  `json:"blockchain_id"`
	Title        string `json:"title"`
	Namespace    string `json:"namespace"`
	ChainId      string `json:"chain_id"`
}

// sendBlockChainsOnce 服务启动时把链配置发送一次到香港集群的 earning_rw_blockchain，
// 之后只走本地缓存，不再发送。
func (s *Scanner) sendBlockChainsOnce(ctx context.Context) error {
	if s.bcWriter == nil {
		log.Warn().Msg("hk automq broker list is empty, skip sending earning_rw_blockchain")
		return nil
	}
	blockChains, err := s.blockChain.List(ctx)
	if err != nil {
		return fmt.Errorf("load block chain list: %w", err)
	}

	msgs := make([]kafka.Message, 0, len(blockChains))
	for _, blockChain := range blockChains {
		if blockChain == nil || blockChain.Id <= 0 {
			continue
		}
		payload := &earningRwBlockChainMsg{
			BlockChainId: blockChain.Id,
			Title:        blockChain.Title,
			Namespace:    blockChain.GetNamespace(),
			ChainId:      chainIdOf(blockChain),
		}
		value, err := json.Marshal(payload)
		if err != nil {
			log.Error().Err(err).Int64("blockchainId", blockChain.Id).Msg("marshal blockchain msg failed")
			continue
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(fmt.Sprintf("%d", blockChain.Id)),
			Value: value,
		})
	}
	if len(msgs) == 0 {
		return fmt.Errorf("block chain list is empty, nothing to send")
	}
	if err := s.bcWriter.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("send earning_rw_blockchain: %w", err)
	}
	log.Info().
		Int("messages", len(msgs)).
		Str("topic", s.cfg.EarningRwBlockchainTopic).
		Msg("block chain sent once")
	return nil
}

// chainIdOf 取链的 chain_id；metadata 缺失时 GetChainId 会返回 "<nil>"，统一按空串处理。
func chainIdOf(blockChain *cmodel.BlockChain) string {
	chainId := blockChain.GetChainId()
	if chainId == "<nil>" {
		return ""
	}
	return chainId
}
