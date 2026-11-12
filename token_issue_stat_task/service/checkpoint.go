package service

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
)

// checkpoint 断点信息（落本地文件 ✓，容器内注意挂 volume ✓）
type checkpoint struct {
	LastTokenId int64  `json:"last_token_id"`
	Scanned     int64  `json:"scanned"`
	Sent        int64  `json:"sent"`
	Finished    bool   `json:"finished"`
	UpdatedAt   string `json:"updated_at"`
}

// loadCheckpoint 读本地断点文件；不存在 / 解析失败都当作从头开始 ✓
func loadCheckpoint(path string) (*checkpoint, error) {
	cp := &checkpoint{}
	if path == "" {
		return cp, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cp, nil
		}
		return cp, fmt.Errorf("read checkpoint %s: %w", path, err)
	}
	if len(raw) == 0 {
		return cp, nil
	}
	if err := json.Unmarshal(raw, cp); err != nil {
		return &checkpoint{}, fmt.Errorf("parse checkpoint %s: %w", path, err)
	}
	return cp, nil
}

// saveCheckpoint 原子写断点文件（先写临时文件再 rename ✓），每批结束调用一次 ✓
func saveCheckpoint(path string, cp *checkpoint) error {
	if path == "" || cp == nil {
		return nil
	}
	cp.UpdatedAt = time.Now().Format("2006-01-02 15:04:05")
	raw, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}

// resetCheckpoint 删除断点文件（从头开始 ✓）
func resetCheckpoint(path string) error {
	if path == "" {
		return nil
	}
	log.Warn().Str("checkpoint", path).Msg("reset checkpoint")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
