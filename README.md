# 固件成组放行 HTTP API（FastAPI + SQLite）

本地单机运行的固件**成组放行（staged rollout）**服务：登记设备组 → 为整组开放行批次 →
设备逐台提交 `ready`/`failed` 回执 → 全部 `ready` 时在**同一事务**内切换所有设备版本。

- 框架：Python 3.12 / FastAPI / uvicorn
- 存储：SQLite（WAL 模式，数据持久化到 Docker volume `/data`）
- 并发：所有写操作使用 `BEGIN IMMEDIATE` 单事务 + 部分唯一索引，并发提交的结果等价于某个串行顺序

## 一条命令启动

```bash
docker compose up --build
```

服务监听 **8080 端口**，健康检查：`GET http://localhost:8080/health`。

交互式 API 文档（Swagger UI）：<http://localhost:8080/docs>

数据库文件位于容器内 `/data/firmware.db`，由 compose 中的 `firmware-data` 命名卷持久化；
删除容器不会丢数据，`docker compose down -v` 才会清除数据卷。

### 不用 Docker 时

```bash
pip install -r requirements.txt
DB_PATH=./firmware.db uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## 业务规则

1. 设备组包含 **2～8 台**唯一设备 ID，每台带当前版本号。
2. 每个批次有唯一批次 ID（服务端生成 UUID），并为组内**每一台**设备指定目标版本。
3. 同组存在未结束（`pending`）批次时不能开新批，返回 `409`。
4. 设备按批次提交回执（`PUT /batches/{batch_id}/devices/{device_id}/receipt`）：
   - 同一设备重复提交**相同结果** → 幂等成功（响应中 `"replayed": true`）；
   - 同一设备改报**另一结果** → `409 Conflict`；
   - 任一 `failed` → 批次立即标记为 `failed`，**所有设备当前版本保持原样**；
   - 全部 `ready` → 在同一事务中把所有设备版本切到目标版本并把批次标记为 `succeeded`，
     任何查询都不可能观察到“部分切换”。
5. 批次结束后只接受**已有回执的重放**；未回执设备的迟到新回执返回 `409`，
   既不能改变本批，也不会影响后续批次。
6. 并发提交由 SQLite 写事务串行化，最终状态必定等价于某个串行执行顺序。

## HTTP 调用示例

以下示例均可用 `curl` 直接执行（假设服务在 `localhost:8080`）。

### 1. 登记设备组

`PUT /groups/{group_id}`，body 中 `devices` 为 `设备ID -> 当前版本`，2～8 个键。

```bash
curl -s -X PUT http://localhost:8080/groups/fleet-a \
  -H 'Content-Type: application/json' \
  -d '{"devices":{"sensor-1":"1.0.0","sensor-2":"1.0.0","sensor-3":"1.2.0"}}'
```

```json
{
  "group_id": "fleet-a",
  "created_at": "2026-09-29T20:39:44.447+00:00",
  "devices": [
    {"device_id": "sensor-1", "current_version": "1.0.0"},
    {"device_id": "sensor-2", "current_version": "1.0.0"},
    {"device_id": "sensor-3", "current_version": "1.2.0"}
  ],
  "open_batch_id": null
}
```

相同内容重复登记是幂等的；设备集合或版本变了则返回 `409`。

### 2. 查询设备组（当前版本 + 未结束批次）

```bash
curl -s http://localhost:8080/groups/fleet-a
```

### 3. 创建放行批次

`targets` 必须恰好覆盖组内全部设备（一台不多、一台不少），值为各自目标版本。

```bash
curl -s -X POST http://localhost:8080/groups/fleet-a/batches \
  -H 'Content-Type: application/json' \
  -d '{"targets":{"sensor-1":"2.0.0","sensor-2":"2.0.0","sensor-3":"2.0.0"}}'
```

```json
{
  "batch_id": "23aec0055f6741b48c38f0476b37b8e4",
  "group_id": "fleet-a",
  "status": "pending",
  "targets": {"sensor-1": "2.0.0", "sensor-2": "2.0.0", "sensor-3": "2.0.0"}
}
```

在该批次结束前再次创建 → `409`。

### 4. 设备提交回执

```bash
# sensor-1 / sensor-2 报 ready
curl -s -X PUT http://localhost:8080/batches/23aec0055f6741b48c38f0476b37b8e4/devices/sensor-1/receipt \
  -H 'Content-Type: application/json' -d '{"result":"ready"}'
# {"batch_id":"...","device_id":"sensor-1","result":"ready","status":"pending","replayed":false,"ended_at":null}

# 重放相同结果：成功且标记 replayed=true
curl -s -X PUT http://localhost:8080/batches/23aec0055f6741b48c38f0476b37b8e4/devices/sensor-1/receipt \
  -H 'Content-Type: application/json' -d '{"result":"ready"}'
# {"...","status":"pending","replayed":true,...}

# 改报另一结果：409
curl -i -X PUT http://localhost:8080/batches/23aec0055f6741b48c38f0476b37b8e4/devices/sensor-1/receipt \
  -H 'Content-Type: application/json' -d '{"result":"failed"}'
# HTTP/1.1 409 Conflict

# 最后一台 ready：整组版本在同一事务内切换，批次 succeeded
curl -s -X PUT http://localhost:8080/batches/23aec0055f6741b48c38f0476b37b8e4/devices/sensor-3/receipt \
  -H 'Content-Type: application/json' -d '{"result":"ready"}'
# {"...","status":"succeeded","replayed":false,"ended_at":"2026-09-29T20:39:44.613+00:00"}
```

失败场景（任一 `failed`，版本全部不变）：

```bash
curl -s -X PUT http://localhost:8080/batches/{batch_id}/devices/sensor-2/receipt \
  -H 'Content-Type: application/json' -d '{"result":"failed"}'
# {"...","status":"failed",...,"ended_at":"..."}
```

批次结束后，未回执设备的迟到新回执被拒绝（`409 batch already failed/succeeded;
late new receipts are not accepted`）；已有回执的重放仍返回 `200` 且 `replayed=true`。

### 5. 查询批次回执明细

```bash
curl -s http://localhost:8080/batches/23aec0055f6741b48c38f0476b37b8e4
```

```json
{
  "batch_id": "23aec0055f6741b48c38f0476b37b8e4",
  "group_id": "fleet-a",
  "status": "succeeded",
  "created_at": "...",
  "ended_at": "...",
  "targets": {"sensor-1": "2.0.0", "sensor-2": "2.0.0", "sensor-3": "2.0.0"},
  "receipts": [
    {"device_id": "sensor-1", "result": "ready", "created_at": "..."},
    {"device_id": "sensor-2", "result": "ready", "created_at": "..."},
    {"device_id": "sensor-3", "result": "ready", "created_at": "..."}
  ]
}
```

### 6. 查询组内批次列表

```bash
curl -s http://localhost:8080/groups/fleet-a/batches
```

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 健康检查 |
| `PUT` | `/groups/{group_id}` | 登记/幂等重登记设备组（2～8 台） |
| `GET` | `/groups/{group_id}` | 查询设备当前版本与未结束批次 ID |
| `POST` | `/groups/{group_id}/batches` | 创建放行批次（有未结束批次时 409） |
| `GET` | `/groups/{group_id}/batches` | 组内批次列表 |
| `GET` | `/batches/{batch_id}` | 批次状态、目标版本、全部回执 |
| `PUT` | `/batches/{batch_id}/devices/{device_id}/receipt` | 提交/重放设备回执 |

状态码：`200` 成功（创建批次为 `201`）、`404` 组/批次/设备不存在、
`409` 冲突（有未结束批次、结果改报、结束后的迟到新回执）、`422` 请求体不合法。

## 一致性与并发是怎么保证的

- 表 `batches` 上建有部分唯一索引：`UNIQUE(group_id) WHERE status = 'pending'`，
  数据库层面保证每组至多一个未结束批次（即使两个创建请求并发也只有一个成功）。
- 每次回执提交都在一条 `BEGIN IMMEDIATE` 事务内完成：插入回执 →
  判断是否“任一 failed / 全部 ready” → 更新版本与批次状态，一起提交或一起回滚，
  读者永远看不到切换到一半的版本。
- `BEGIN IMMEDIATE` 让写者一开始就拿写锁，配合 `busy_timeout=10000` 排队等待，
  不会出现 SQLite 的死锁升级；并发执行必然等价于某种串行顺序。
- 回执表主键为 `(batch_id, device_id)`，重放相同结果直接返回既有记录，
  改结果因行已存在而冲突；批次结束后对“无既有回执”的新插入一律拒绝。

## 测试

```bash
pip install -r requirements.txt
pip install pytest pytest-asyncio httpx
pytest -q
```

共 19 个测试，含 4 个基于真实 uvicorn 线程池的并发测试（全部 ready 并发、
ready/failed 混合并发、同设备重放竞争、并发开批只有一个胜出）。
