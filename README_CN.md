# fusion-flow（中文文档）

面向 Fusion 生态的进程内持久化任务编排库：持久队列 + 可审计状态机 + 按 kind 限流 +
指数退避重试 + 崩溃自愈 worker——全部落在一个 SQLite 文件上，**零内部 `fusion-*` 依赖**。

运行时依赖仅 `anyio` 与 `python-ulid`。

- **默认持久**——每次状态迁移都提交进 SQLite（WAL）；`kill -9` 打断的任务由 claim 超时回收自愈。
- **单写者、无竞态**——每个数据库只允许一个进程跑 worker（边车文件锁强制）；claim/ack/nack 均为原子条件更新。
- **全程可审计**——每次迁移与事件同事务落库；事件回调在提交后触发，永远不可能回滚队列状态。
- **可续跑**——retry（死信）与 replay（已成功）复制出新 Job（`attempts` 归零），原记录与审计链不动。

## 五分钟上手

```bash
cd /path/to/fusion && source .venv/bin/activate
cd fusion-flow && pip install -e ".[test]"
```

```python
import anyio
from fusion_flow import Flow, JobSpec

flow = Flow("jobs.db", workers=2)


@flow.handler("resize_image")
async def resize(ctx, payload):
    # ... 干活 ...
    return "s3://results/out.png"  # 返回引用，不返回数据本体（见下文）


async def main():
    async with flow.run():  # 拿 worker 锁，启动 worker 池
        job = flow.enqueue(JobSpec(kind="resize_image", payload={"uri": "..."}))
        print(job.id, job.status)  # flw_... queued


anyio.run(main)
```

同步 handler 也支持——跑在有界线程池里：

```python
@flow.handler("ocr")
def ocr(ctx, payload):  # 普通 def，不是 async def
    return "ref://ocr-result"
```

要点：

- `flow.enqueue()` / `flow.get_job()` / `flow.stats()` 是**同步** API，业务代码直接调；
  `flow.run()` 是唯一异步入口。
- `dedup_key="file:42"`：同一逻辑任务在 queued/processing 期间重复入队会复用同一 Job；
  再加 `dedup_ttl=timedelta(minutes=5)` 可复用近期**已成功**的结果（返回的 Job
  `created=False`，带旧 `result_ref`）。
- `clock=Clock(seed=...)` 可注入：退避、claim 过期、限流全部读它，测试从不真 sleep。

## Handler 异常分类

worker 的异常处理是固定契约，务必有意识地选分支：

```python
from fusion_flow.errors import PermanentError, TransientError


@flow.handler("mockup_batch", fail_fast=True)
async def mockup_handler(ctx, payload):
    try:
        ...
    except (InputShapeError, DegenerateQuadError, UnsupportedModeError) as e:
        # 业务判定：输入本身有问题，重试无意义
        raise PermanentError(f"素材参数错误，不可重试: {e}") from e
    except OSError as e:
        # 基础设施抖动：退避重试
        raise TransientError("IO 临时故障，可重试") from e
    # 其他未预期 Exception：
    #   普通 handler  -> nack + 退避重试，耗尽进死信
    #   fail_fast=True -> 立即 dead_lettered + fatal 事件（供告警接线）
```

决策口诀：

| 异常 | 结果 | 消耗重试次数？ |
| --- | --- | --- |
| `TransientError` | nack、退避、重新排队 | 是 |
| 未预期 `Exception`（普通 handler） | nack、退避、重新排队 | 是 |
| `PermanentError` | `failed`（业务终态） | 否 |
| 任意 `Exception` 且 `fail_fast=True` | `dead_lettered` + `fatal` 事件 | 否 |
| `KeyboardInterrupt` / `SystemExit` | 停池、异常上抛；Job **绝不** nack | 否 |

- `failed` = 业务判定该任务永不可能成功；
- `dead_lettered` = 重试耗尽（或致命错误）。修因后 `flow retry <id>` 复制新任务。

## Drain 超时会消耗重试次数（加粗警告）

**`flow.run()` 退出时，在途 async handler 有 `drain_timeout_s`（默认 30 秒）宽限期；
超时被取消的 async job 会以 `last_error="worker_drain_timeout:cancelled"` 被 nack——
该 nack 使 `attempts+1`，消耗一次重试。**

同步 handler 完全无法取消：线程继续跑，job 被遗留在 `processing`，之后由 claim 超时
回收重新入队——回收同样计一次 attempt。设置 `max_retries` 与 `drain_timeout_s` 时
必须把这一点算进去。

## 限流是 best-effort、纯内存

```python
flow.rate.set_rate("drive_poll", "2/min")  # 或 Flow(..., rate_per_kind={...})
```

令牌桶在进程内存里。后果：

- **进程重启即清零**——重启后可能立刻冲出一波超限突发；
- 每个数据库只允许一个 worker 进程（锁强制），但**多个入队进程**各自持有独立桶视图；
  跨进程限流是调用方的职责，不是库的。

## SQLite 持久化权衡（`synchronous=NORMAL`）

默认 PRAGMA：`journal_mode=WAL`（不可覆盖）、`synchronous=NORMAL`、`busy_timeout=5000`、
`foreign_keys=ON`。

- **进程崩溃（kill -9）：已提交事务一条不丢**——claim 回收机制就围绕这种崩溃设计；
- **物理断电：最近的提交可能丢**（WAL 尚未 fsync）。

绝对零丢失场景，开库时覆盖：

```python
flow = Flow("jobs.db", sqlite_pragmas={"synchronous": "FULL"})  # 写吞吐下降
```

## Payload 约束：只放 id / uri

`payload` 有上限（默认 64 KiB，硬顶 1 MiB），且去重指纹逐字节哈希它。**只放标识符和引用**：

```python
JobSpec(kind="resize", payload={"uri": "file:///data/in.png"})  # 正确
JobSpec(kind="resize", payload=my_8mb_image_bytes)  # PayloadTooLarge，且语义就错
```

大输入大输出放对象存储/文件系统，队列只传指针。handler 返回值同理：返回
`result_ref` 字符串（`ref://...`、`s3://...`），不返回数据本体。

## Worker 规模与可存活于 drain 的 handler 写法

- **workers=2 起步**。预算公式：`workers × 单任务峰值内存 < 可用 RAM`。
  内存大户（如图像流水线）加 workers 前必须先算这笔账，否则 OOM killer 替你算。
- **优先 async handler**。同步 handler 里的长 CPU 计算无法被 drain 超时取消
  （线程杀不掉），且会卡住 `flow.run()` 直到它跑完。async 取消干净并 nack 重试。
- 所有 handler 必须**幂等**：一个 job 可能跑多次（崩溃后重试、drain 取消后重试、
  claim 超时回收）。

## Connector（旁路数据源）

Connector 把外部数据源（如 Google Drive）桥接进队列。`run()` 之前注册，runner 作为
独立任务与 worker 池并行：

```python
from fusion_flow import DriveConnector


def on_new_file(folder_id, file):
    # 目录 -> 业务映射在调用方这里，库不理解业务。
    return JobSpec(kind="photo", payload={"file_id": file["id"]}, dedup_key=f"drive:{file['id']}")


conn = DriveConnector(
    drive_client,
    on_new_file=on_new_file,
    event_bus=flow.event_bus,
    inbox_dir="./inbox",
    output_folder_id="...",
    rejected_folder_id="...",
)
flow.attach_connector(conn, name="drive", interval_s=60, kinds={"photo"})
```

`Connector` 协议三个方法：`poll() -> list[JobSpec]`、`commit()`、
`on_job_done(job, result_ref)`。契约要点：

- **零丢失背压**：任一 enqueue 抛 `QueueFull`，**整轮 poll 视为失败**——`commit()`
  不被调用，connector 游标（Drive pageToken）原地不动，下一轮重拉同一批文件。
  队列满时绝不丢弃 spec——上游不会替你重推。
- **非法 spec 丢弃但不致命**：业务回调产出的超大 payload / 非法 kind 发
  `connector_spec_dropped` 事件后丢弃，本轮继续（毒 spec 不能永久卡死 connector）。
- **完全隔离**：`poll()` 抛异常永远不影响 worker——记 `connector_error` 事件并退避
  （基数 5s，封顶 300s）。已入队任务照常消费。
- **反向通路**：`on_job_done` 在 ack 后触发（按 `kinds` 过滤），带退避重试；重试耗尽
  发 `connector_error` 事件供补偿——**Job 终态绝不回滚**。
- `DriveConnector` 的 pageToken 存 `flow_kv`，**永不设 TTL**（这是持久游标不是缓存）；
  传输 client 由调用方注入——库本身零 Google import，`pip install "fusion-flow[drive]"`
  只为调用方侧的适配器装客户端库。

## 故障排查速查表

| 症状 | 排查 |
| --- | --- |
| Job 卡在 `processing` | `flow show <id>` 看 `claim_expires_at`——过期后调度器自动回收；可用 `flow lease-reclaim` 手动触发 |
| `flow.run()` 报 `WorkerLockHeld` | 另一个活进程占用该库的 worker：`flow locks` 查 `<db>.worker.lock` |
| `database is locked` 报错 | 多进程同时写；每库只允许一个进程 `flow.run()`，读走 CLI |
| 限流"不生效" | 进程重启过（桶清零）或存在多个入队进程；限流器设计上就是进程内存态 |
| 死信修好了想重跑 | `flow retry <id>` 复制成全新 queued job，`attempts=0` |
| Connector 不再入队 | 事件列表里的 `connector_error`；runner 指数退避并持续重试 |
| 磁盘增长 | `flow purge --older-than 7d` 清终态任务，`flow vacuum` 回收空间 |

## CLI 参考

```text
flow --db jobs.db <command>            # --db 默认 $FLOW_DB 或 flow.db；全部支持 --json

  ls [--status S] [--kind K] [--limit N]   列出任务
  show <job_id> [--events N]               任务详情 + 子任务 + 审计事件
  retry <job_id>                           死信任务复制为新 queued 任务
  replay <job_id>                          已成功任务复制为新一次运行
  cancel <job_id>                          取消 queued 任务
  purge --older-than 7d --statuses succeeded,cancelled
  stats                                    队列快照（按状态、年龄、吞吐）
  vacuum                                   VACUUM 数据库
  locks                                    worker 锁状态
  lease-reclaim                            回收过期 claim 重新入队
```

退出码：`0` 正常，`1` FlowError，`2` 未找到。CLI 从不抢 worker 锁，只在锁被活
worker 持有时向 stderr 打告警。

## 开发

```bash
cd /path/to/fusion && source .venv/bin/activate
cd fusion-flow
pip install -e ".[test]"
pytest tests/ -q                # 全量用例（含 kill -9 崩溃测试）
pytest tests/ -q -m bench       # 吞吐冒烟基准（CI-only 标记）
ruff check . && ruff format --check .
```

- Python >= 3.12，4 空格缩进，无 docstring（用注释），日志必带。
- SQLite 是唯一存储：`storage.py` 是唯一允许执行 SQL 的模块。
- 迁移：`migrations.py` 每个版本一条，up + down 成对；`flow_migrations` 簿记表
  在回滚后保留。

Apache-2.0 协议。英文文档见 [README.md](README.md)。
