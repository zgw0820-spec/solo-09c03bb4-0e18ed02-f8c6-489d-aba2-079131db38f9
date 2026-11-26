# 制品库 · 双卷一致性备份台（FastAPI + Docker Compose）

制品库的 **索引（index）** 与 **文件（blobs）** 分别落在两个独立的卷上，恢复时
必须使用 **同一备份轮次（round）** 的两份快照。本项目提供：

- 一个 FastAPI 备份页面（原生 JS，无外部 CDN/构建）；
- 可发布“样例制品”：每个制品分两次落盘（先文件卷、间隔可调、后索引卷），间隔在页面/API 可调；
- 双卷快照端各一个，**真实复制挂载卷内容**，可运行时调延迟、注入失败；
- 严格的备份状态机：暂停接单 → 等在途制品写齐两卷 → 最多 **3 秒** 的停写抓取窗口等两卷回报 → 两卷按时成功才提供整组 zip；成功/失败/超时都立即恢复接单；
- **排队发布（预约）**：暂停接单期间可先排队、不写盘、可撤回；预约满 **10 秒** 未被接单自动作废；
  恢复接单时先按预约顺序接下仍有效的预约，再接新单；紧接着的下一轮会先把这些已接单制品
  排空（写齐两卷）再抓取。预约只存在 Web 进程内存，不跨重启；
- 轮次隔离：迟到回报只追加到原轮次的审计列表，**不改原结果、不碰接单开关、不干扰下一轮**；
- 同一轮校验的恢复脚本。

---

## 1. 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

打开 <http://127.0.0.1:8000>。

| 服务 | 地址（宿主机映射） | 说明 |
| --- | --- | --- |
| `web` | <http://127.0.0.1:8000> | 备份页 + API + 整组 zip 下载 |
| `snap-index` | <http://127.0.0.1:18001> | 索引卷快照端（调试用，页面已内置代理） |
| `snap-blob` | <http://127.0.0.1:18002> | 文件卷快照端（调试用） |

Compose 用三个 bind mount 分别充当两块业务卷和一块快照共享卷：

```
./data/index      -> web:/volumes/index,  snap-index:/source:ro
./data/blobs      -> web:/volumes/blobs,  snap-blob:/source:ro
./data/snapshots  -> web:/snapshots,      两个快照端:/snapshots
```

快照落在 `./data/snapshots/<round_id>/{index,blobs}/`，整组 zip 落在
`./data/snapshots/bundles/<round_id>.zip`。想换成 Docker named volume，把
`docker-compose.yml` 里的 bind 路径替换成命名卷并在底部声明 `volumes:` 即可。

### 没有 Docker 时的本地等价跑法

仓库自带脚本，用三个本地进程 + 三个真实目录复刻 Compose 拓扑：

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
scripts/run-local.sh                 # 默认数据目录 ./data-local
# 或指定目录：scripts/run-local.sh /tmp/vault-data
```

然后：

```bash
python3 scripts/trial.py             # 纯标准库，跑 A/B/C 三组实况断言
```

---

## 2. 页面操作

1. **发布样例制品**：设置“两次落盘间隔（秒）”→ 发布。明细表能看到文件卷与索引卷
   各自真实的落盘时间戳和状态（写文件卷 → 间隔等待 → 写索引卷 → 两卷写齐）。
2. **快照端面板**：分别调两个卷的回报延迟、选择“持续失败”，或点“下一次抓取失败”
   （一次性注入）。
3. **发起双卷备份**：
   - 已有轮次在跑时按钮禁用，直接调 API 会得到 `409 round_already_active`；
   - 备份一开始接单立即暂停，备份期间发布会得到 `409 acceptance_paused`；
   - 页面实时显示排空耗时、3 秒窗口倒计时、两卷回报、迟到回报、失败原因；
   - 成功后出现“下载整组快照 zip”。
4. **排队发布（暂停接单期间）**：
   - 接单暂停后，“发布样例制品”按钮禁用，旁边的 **“排队发布”** 按钮可用。点它只会
     生成一条预约（`rsv_...`），**不写任何卷**；预约表显示状态与剩余秒数，
     值班员不用等本轮结束再回来点发布；
   - 排队中的预约可点 **撤回**：撤回后该单永远不会发布；
   - 预约自排队起 **10 秒** 内未被接单即自动 **作废**（页面倒计时归零、状态变红），
     作废后同样永不发布，也不能再撤回；
   - 本轮备份无论成功、失败还是超时，恢复接单时会**先按预约顺序接下仍有效的预约**，
     再接新单。若马上发起下一轮，下一轮会把刚接下的预约制品当作“暂停时在途”，
     等它们写齐两卷后才抓取；
   - 预约只保存在 Web 进程内存中，**不跨 Web 重启**（重启后列表清空，旧预约不会被发布）。
5. 面板上还有四个一键试跑按钮：
   - **试跑 A**：发一个 1.2s 间隔的在途制品并立刻备份，观察“先排空、后抓取、成功下载”；
   - **试跑 B**：把索引卷延迟调到 4.5s 制造超时，首轮失败后立刻发起第二轮，
     随后可看到首轮的“迟到成功回报”被归档但不改判定，第二轮照常成功。
   - **试跑 C**：备份期间排队两单、撤回一单，观察恢复接单时保留单按序被接单、撤回单从未发布；
   - **试跑 D**：用一个 14s 的在途制品让暂停持续更久，排队一单并等到 10s 自动作废。

---

## 3. 一致性协议（关键设计）

```
接单中 accepting=true
   │  POST /api/backup/start（同步临界区：检查无活动轮次 → accepting=false）
   ▼
draining：等待暂停瞬间所有在 pending 集合中的制品完成两次落盘
   │  （写失败 / drain 超时 → failed，立即恢复接单，不供下载）
   ▼
snapshotting：两卷已无在途写 → 开启停写窗口（SNAPSHOT_WINDOW_SECONDS，硬上限 3.0s）
   │  并行 POST 两个快照端 /snapshot（带 round_id）
   │  · 任一卷在窗口内回报失败         → 立即 failed(agent_failure) + 恢复接单
   │  · 到 3s 未收齐两个成功回报        → failed(capture_timeout) + 恢复接单
   │  · 两个成功回报都在窗口内到达      → success + 恢复接单 + 打整组 zip
   ▼
success / failed（终态，判定只发生一次）
   │  恢复接单顺序（同步完成，中间不 await）：
   │    ① 先把已过 10s 的预约标记 expired
   │    ② 按预约顺序把仍有效的预约转为已接单（加入 pending，立即开始落盘）
   │    ③ 最后才打开 accepting=True，允许新单/下一轮
   ▼
```

### 排队发布（预约）语义

- **暂停期间才有队列**：`POST /api/publish-queue` 在接单中返回 `409 not_paused`。
  原 `POST /api/artifacts` 在暂停期间依旧返回 `409 acceptance_paused`（行为不变）。
- **排队不写盘**：预约只是内存里的记录，被接单前不会生成制品、不会写 index/blob 卷。
- **10 秒 TTL**：每个预约有自己的计时器（到截止时刻按墙钟计算，早醒会续睡），
  同时状态查询与恢复接单入口都会先做一次“到期清扫”，因此即使计时器被错过，
  过期预约也绝不会被接单。到期 → `expired`，不再发布。
- **撤回/作废终态**：`withdrawn` / `expired` 都是终态，后续恢复接单只挑 `queued`；
  对非 `queued` 的再次撤回返回 `409 reservation_not_queued`（附带当前状态）。
- **顺序与下一轮**：恢复接单时先把仍有效的预约按排队顺序转成制品（都进入 `pending`），
  然后才 `accepting=True`。下一轮 `start_backup` 在暂停瞬间把 `pending` 快照为
  `inflight_at_pause`，因此紧接着开始的下一轮会先等这些预约制品写齐两卷再抓取。
- **不跨进程**：预约表随 Web 进程存在；重启后为空的队列，旧 id 撤回返回 404。

关于“最多停 3 秒”的口径：**3 秒是“两卷写齐之后、卷面静止的停写抓取/回报窗口”的
硬上限**（`SNAPSHOT_WINDOW_SECONDS` 只允许调小、代码里 clamp 到 ≤ 3.0）。在排空
阶段虽然已经“暂停接单”，但已接单制品仍在继续做两次落盘，因此这段时间不属于
“停写期间”；若把 3 秒理解成包含排空的总暂停上限，就无法同时满足“等在途制品写齐
两卷再抓取”和可调的两次落盘间隔。页面与 API 同时展示排空耗时和窗口倒计时，两段
时间可分别核对。

**迟到回报隔离**：窗口超时/失败结算时，尚未返回的快照 HTTP 任务不会被取消，而是
挂一个只写旧轮次 `late_reports` 的 watcher。结算函数有终态保护（只能成功一次），
恢复接单带轮次身份判断（`active_round_id == rnd.id` 才清空）。新轮次是全新的
`Round` 对象和全新的任务集合，因此旧轮次 4.5s 的迟到响应既改不了旧结果，也碰不到
新轮次。

**同轮证据**：快照端复制完卷后会在快照目录写 `_snapshot_report.json`（含
`round_id`、文件清单和 sha256）；整组 zip 的 `manifest.json` 记录同一 `round_id`。

### 同轮恢复

```bash
scripts/restore.py data/snapshots/bundles/<round_id>.zip \
  --index-dir /restore/index --blob-dir /restore/blobs
```

脚本会核对 manifest 与两卷快照戳记的轮次号，不一致直接拒绝（退出码 3），
避免把不同轮次的索引和文件混着恢复。

---

## 4. HTTP API

| 方法 & 路径 | 说明 |
| --- | --- |
| `GET /` | 备份页面 |
| `GET /api/state` | 全量状态：接单开关、在途、设置、制品、轮次（含迟到回报）、事件 |
| `POST /api/artifacts` | 发布样例制品；body 可选 `{"name","interval_seconds"}`；暂停时 409 |
| `POST /api/publish-queue` | 暂停接单期间排队发布；body 同上；返回预约（不写盘）；接单中无需排队时 409 `not_paused` |
| `POST /api/publish-queue/{id}/withdraw` | 撤回排队中的预约；非排队状态 409，未知预约 404 |
| `POST /api/settings` | 更新默认两次落盘间隔 |
| `POST /api/backup/start` | 发起一轮双卷备份；已有活动轮次时 409 |
| `GET /api/rounds/{id}` | 单轮详情 |
| `GET /api/rounds/{id}/download` | 下载整组 zip；非 success / 包未就绪时 409 |
| `GET /api/agents` | 两个快照端的连通性与故障注入配置（代理） |
| `PUT /api/agents/{index|blobs}/config` | `{"delay_seconds?","fail_mode?","fail_once?"}` |
| `GET /health` | 健康检查 |

快照端自身另有 `POST /snapshot`、`GET/PUT /admin/config`、`GET /health`。

### 环境变量

Web：`INDEX_DIR`、`BLOB_DIR`、`SNAPSHOT_DIR`、`BUNDLE_DIR`、
`SNAP_INDEX_URL`、`SNAP_BLOB_URL`、`SNAPSHOT_WINDOW_SECONDS`（>3 会被压回 3）、
`WRITE_INTERVAL_SECONDS`、`DRAIN_TIMEOUT_SECONDS`、`AGENT_HTTP_TIMEOUT_SECONDS`、
`RESERVATION_TTL_SECONDS`（默认 10；仅测试/演示调小）。

快照端：`VOLUME_LABEL`、`SOURCE_DIR`、`SNAPSHOT_ROOT`、
`AGENT_DELAY_SECONDS`、`AGENT_FAIL_MODE`、`AGENT_FAIL_ONCE`。

---

## 5. 测试与实测结果

### 自动化测试（ASGI 全链路 + 真实临时目录复制/打包）

```bash
.venv/bin/python -m pytest tests/ -v
```

12 个用例全部通过（约 11s）：

- 页面可访问；
- 两次落盘间隔真实生效且两卷文件都存在；
- 在途发布 → 备份先排空 → 成功 → 两卷同轮快照目录 + 整组 zip/manifest；
- 并发两个 start 只有一个 201，备份中发布 409；
- **抓取超时（0.8s 测试窗口 + 1.4s 索引延迟）→ 判 `capture_timeout`、立即恢复、
  不可下载；随后的迟到成功回报只归档，原结果不变，下一轮成功且零迟到记录**；
- **一卷注入失败立即 `agent_failure` 并恢复，另一慢卷的成功转迟到归档**；
- 设置更新与非法 `round_id`（路径穿越）被快照端拒绝；
- **暂停期间排队：接单中排队 409；排队期间两卷无任何写入、无制品记录；
  撤回后本轮结束也不发布；重复撤回 409**；
- **过期预约：TTL 内未被接单自动 `expired`，不能撤回，恢复接单后仍不发布**；
- **恢复接单按预约顺序接单：两个预约的制品 seq 递增，新单 seq 更晚；
  紧接着的下一轮把它们算作暂停时在途并完成排空后才抓取，两卷最终写齐**；
- **失败轮（`capture_timeout`）恢复接单时同样先接下有效预约，下一轮先排空它们**；
- **不跨重启：同一个 Settings/快照端上新建 app，状态里预约为空、旧 id 撤回 404**。

### 三进程实况试跑（真实 uvicorn + HTTP + 真实 3 秒窗口）

```bash
scripts/run-local.sh /tmp/vault-queue
python3 scripts/trial.py
```

最近一次结果：**52 项断言全部通过**，关键实测数字：

- 在途制品间隔 2.00s，备份排空耗时 **1.70s**（备份在发布后 0.3s 发起），随后成功；
- 索引卷延迟 4.5s 时，轮次在 **3.05s** 判 `capture_timeout`（不是等到 4.5s），
  立即恢复接单、下载 409；
- 调回延迟后第二轮成功；约 4.5s 时首轮迟到回报到达并归档，
  首轮仍为 `failed/capture_timeout`，次轮 `success` 且无迟到记录；
- 一次性失败注入后 **0.21s** 判 `agent_failure` 并恢复接单；
- 排队：恢复后保留单 `accepted`、撤回单 `withdrawn` 且无制品；
  紧接着下一轮 `drain=0.49s`、暂停时在途含该预约制品；
- 作废：预约在 **10.04s** 自动 `expired`，本轮排空 14s 在途后成功，作废单始终未发布；
- 成功轮次才有 `bundles/*.zip`，超时/失败轮没有包；
- `scripts/restore.py` 从真实 zip 恢复两卷成功；篡改 manifest 轮次号后被拒绝（exit 3）。

### 页面 DOM 冒烟（jsdom 连真实后端）

27 项检查通过：轮次/代理/制品渲染、点击发布后出现“两卷写齐”、备份按钮互斥、
**暂停时“排队发布”可用而直接发布禁用、预约表渲染排队中状态、点击撤回后变“已撤回”
且撤回按钮消失、保留单在本轮结束后显示“已接单”并关联制品、撤回单从未出现在制品表**、
交互后无 JS 报错、轮询重渲染不会清掉正在输入的延迟值。

---

## 6. 目录结构

```
app/
  main.py                    # FastAPI 工厂、API、下载
  manager.py                 # 发布写入 + 备份轮次状态机/预约队列/迟到隔离/打包
  config.py                  # 环境配置与 3s 硬上限、10s 预约 TTL
  static/index.html          # 备份页（原生 JS/CSS，含排队发布/撤回）
  agent/snapshot_agent.py     # 快照端工厂 + uvicorn 入口（真实 copytree + 故障注入）
tests/test_flows.py          # 12 个 ASGI 全链路用例
scripts/run-local.sh         # 无 Docker 的三进程启动
scripts/trial.py             # 实况 A–E 断言脚本（纯 stdlib）
scripts/restore.py           # 同轮校验恢复工具
Dockerfile  docker-compose.yml  requirements.txt
```

## 7. 已知边界 / 未完成项（如实说明）

- 本沙箱**没有安装 Docker / 守护进程**，因此 `docker compose up --build` 未能在
  此环境实际执行；`docker-compose.yml` 已通过 YAML 解析与人工核对，并用完全相同的
  代码以三进程本地拓扑完成了实况验证。首次在有 Docker 的机器上构建时可能需要按
  基础镜像拉取情况稍等。
- 快照复制目前是全量 `copytree`（带 sha256 清单），没有做增量快照/硬链接去重；
  样例制品很小，生产化时应换增量或存储级快照。
- 没有加鉴权/多用户与 HTTPS；定位是本机/内网的一致性演示与运维台。
- 状态保存在进程内存 + 卷上的快照/zip；Web 重启后历史轮次元数据不保留
  （快照目录与 zip 文件仍在卷上）。
- 没有内置定时备份与保留期清理策略；备份严格手动/API 触发，且一次一轮。
- 排队发布预约**只存在 Web 进程内存**：多副本/多 worker 时各进程各有一份队列，
  重启即丢失（这是本需求的明确口径）；若要多实例共享需外接队列/存储。
- 预约在“恢复接单”那一刻按墙钟判定是否过期：恰好卡在 10s 边界上的预约由
  `time.time()` 比较决定，行为是确定性的（≥10s 即作废），但不保证毫秒级同时。
- 试跑脚本在高负载宿主机上偶发出现快照复制逼近 3s 窗口而判 `capture_timeout`
  （原始基线代码同样可复现，与本次改动无关）。这是全量 `copytree` 在慢盘/高负载下
  的耗时特性，**不是**排队发布逻辑引入的；连续 20+ 轮实况重复试跑中仅个别轮次命中。
