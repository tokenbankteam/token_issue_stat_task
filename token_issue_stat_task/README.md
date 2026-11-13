# token_issue_stat_task

独立服务：先扫 `t_token_extension` 中 **issuer 非空** 的记录，再按 token_id 取 `t_token`，
组装 `commonmodel.TokenStatTaskMsg`（issue / issue_time / holder_count / market_cap）
推送到 swap_token_search_task 消费的 topic（下游写 ES）。

## 数据来源

```sql
-- ① 第一段：issuer 非空，按主键 token_id 游标分页
SELECT token_id, issuer, issue_time, circulating_supply
FROM t_token_extension
WHERE issuer IS NOT NULL AND issuer <> ''
  AND token_id > ?
ORDER BY token_id
LIMIT ?                  -- batchsize = 200

-- ② 第二段：按这批 token_id 取基础信息（单次 IN ✓）
SELECT id, blockchain_id, address, bl_symbol, holder_count
FROM t_token
WHERE id IN (?)

-- ③ 价格：调 tb_server POST /v1/token/token_price_list（单次最多 100 个，内部按 100 分批 ✓）
--    请求体：[{blockchain_id, address, bl_symbol}, ...]
--    响应：  {"result":0,"data":["1.23","0", ...]}  ← 与请求顺序一一对应 ✓
```

## 扫描节奏

```
每批 200 个 token  →  sleep 500ms  →  ★ 每批落一次断点（本地文件 ✓）  →  一轮扫完打印 `scan success, exit` 后进程退出 ✓
连接池 maxopenconns 默认 4（只读，避免和其它服务抢连接）
```

## 消息格式

```json
{
  "tk": { "blockchain_id": 27, "address": "...", "bl_symbol": "..." },
  "attr": {
    "issue": "xxx",
    "issue_time": 1698765432,
    "holder_count": 1300,
    "market_cap": 12345.6789
  }
}
```

- `market_cap = circulating_supply × price_usd`
  （`circulating_supply` 来自 `t_token_extension`；为空/0/非法/负数或价格拿不到时 market_cap=0，消息里不带该字段 ✓）
- 下游 `swap_token_search_task` 落 ES 字段：`iss` / `ist` / `hc` / `mc`

## 运行

```bash
go build -o token_issue_stat_task .
./token_issue_stat_task -config config.yaml
```

## 配置

```yaml
db:                  host / port / user / password / database、maxopenconns、maxidleconns
brokerlist:          kafka 集群（与 swap_token_search_task 相同）
tokenstatresulttopic: swap_token_search_task 消费的 topic
tbserveraddr:        tb_server 地址（价格接口 /v1/token/token_price_list）
tbservertimeoutseconds: 价格接口超时，默认 5s
scanner: enabled、batchsize(200)、sleepmillis(500)、resume(断点续扫)、reset(清空断点)
```

## 断点续扫（本地文件）

每批结束把进度写到本地断点文件（默认 `./checkpoint.json`），进程挂了重启会从 `last_token_id` 继续 ✓：

```json
{
  "last_token_id": 41234567,
  "scanned": 980000,
  "sent": 12000,
  "finished": false,
  "updated_at": "2026-11-02 16:40:00"
}
```

- 原子写（先写 `.tmp` 再 rename ✓），不会因为写一半坏掉 ✓
- 启动 `resume: true` → 读断点续扫 ✓（日志 `resume from checkpoint` ✓）
- 中断：断点保留 ✓，直接重启即可 ✓
- 扫完：`last_token_id=0` + `finished=true` ✓，下次从头 ✓
- 重跑：`reset: true` 或手动 `rm checkpoint.json` ✓

```yaml
scanner:
  resume: true
  reset: false
  checkpointfile: "./checkpoint.json"
```

> ⚠️ 容器部署必须把断点文件放到**持久化 volume** ✓，否则重启 pod 断点就没了 ✗。

## 物理机部署（非 k8s）

### 打包

```bash
./build.sh                          # 产出 Linux amd64 静态二进制 ✓
# 或指定：
GOOS=linux GOARCH=arm64 ./build.sh token_issue_stat_task_arm64
```

### 目录建议

```
/data/token_issue_stat_task/
├── token_issue_stat_task      # 二进制
├── config.yaml
├── checkpoint.json            # 断点（自动生成 ✓ 放在本地盘 ✓）
└── logs/run.log
```

### 直接跑

```bash
cd /data/token_issue_stat_task
nohup ./token_issue_stat_task -config config.yaml >> logs/run.log 2>&1 &
# 扫完会自己退出 ✓（exit 0）
```

### systemd（一次性任务）

```bash
cp deploy/token_issue_stat_task.service /etc/systemd/system/
systemctl daemon-reload
systemctl start token_issue_stat_task        # 跑一次，扫完自动退出
systemctl status token_issue_stat_task
```

> 断了直接再跑一次即可 ✓ —— 断点文件在本地盘 ✓，会从 `last_token_id` 继续 ✓。
> 想从头重扫：`rm checkpoint.json` 或 `reset: true` ✓。
