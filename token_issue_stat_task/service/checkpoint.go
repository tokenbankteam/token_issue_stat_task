package service

import (
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tokenbankteam/tb_common/cache"
)

// 默认断点 key（可在配置 redis.checkpointkey 覆盖）
const defaultCheckpointKey = "token_issue_stat_task_checkpoint"

// checkpointTTL 断点有效期：7 天
// 每次保存都会刷新 TTL（滑动窗口）：扫描过程中不会过期，最后一次写入后 7 天自动清理
const checkpointTTL = 7 * 24 * time.Hour

// checkpoint 断点信息（存 Redis，跨 Pod 重建也能续扫 ✓）
type checkpoint struct {
	LastTokenId int64  `json:"last_token_id"`
	Scanned     int64  `json:"scanned"`
	Sent        int64  `json:"sent"`
	UpdatedAt   string `json:"updated_at"`
}

// loadCheckpoint 从 Redis 读断点；key 不存在 / 解析失败都当作从头开始 ✓
func loadCheckpoint(rdb *cache.Cache, key string) (*checkpoint, error) {
	cp := &checkpoint{}
	if rdb == nil || key == "" {
		return cp, nil
	}
	raw, err := rdb.Get(key).Result()
	if err != nil {
		// 包含 redis.Nil（key 不存在），一律视为从头扫
		return cp, nil
	}
	if raw == "" {
		return cp, nil
	}
	if err := json.Unmarshal([]byte(raw), cp); err != nil {
		log.Warn().Err(err).Str("key", key).Msg("parse checkpoint failed, fallback to full scan")
		return &checkpoint{}, nil
	}
	return cp, nil
}

// saveCheckpoint 每批结束写一次 Redis ✓
func saveCheckpoint(rdb *cache.Cache, key string, cp *checkpoint) error {
	if rdb == nil || key == "" || cp == nil {
		return nil
	}
	cp.UpdatedAt = time.Now().Format("2006-01-02 15:04:05")
	raw, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return rdb.Set(key, string(raw), checkpointTTL).Err()
}

// resetCheckpoint 删除断点（扫完 / 强制重扫时调用 ✓）
func resetCheckpoint(rdb *cache.Cache, key string) error {
	if rdb == nil || key == "" {
		return nil
	}
	log.Warn().Str("key", key).Msg("reset checkpoint")
	return rdb.Del(key).Err()
}

// checkpointKey 取配置里的 key，缺省用默认值
func checkpointKey(cfgKey string) string {
	if cfgKey == "" {
		return defaultCheckpointKey
	}
	return cfgKey
}
