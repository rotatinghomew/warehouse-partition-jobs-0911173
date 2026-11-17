# 按地区业务日运行的活动数仓作业

这是一个处理本地数据交换的仓湖项目。示例使用虚构机构和匿名合成记录，不连接业务平台。

## 数据契约

源分区键为地区与地区业务日期，每次不同内容产生新版本，相同内容重传复用旧版本。当前作业为显式指定输入的日汇总与周汇总，周为周一起始的业务周。发布名称绑定一组不可变作业结果，运行作业不自动覆盖发布。日界映射使用IANA时区，不接受无时区时间。

## 代码职责

- `partitionflow/calendar.py`：地区业务日、UTC边界与周归属。
- `partitionflow/engine.py`：显式输入的源、日与周结果；`*_tx` 为事务内核心，供运行器在单事务内组合。
- `partitionflow/publication.py`：固定输入版本的查询发布与补算元数据表结构。
- `partitionflow/storage.py`：本地事务、内容摘要和查询工具。
- `partitionflow/plan.py`：按真实输入依赖固定源版本、缺口识别、补报解释与血缘。
- `partitionflow/runner.py`：补算运行、失败恢复、防陈旧发布与当前结果的原子一致切换。

## 安装与运行

使用 Python 3.11（兼容该系列补丁版本，独立容器镜像以 SHA256 摘要固定）。构建工具和运行依赖均按精确版本登记。先执行 `python3 -m pip install -r requirements-build.txt -r requirements.txt`；依赖仅在准备环境时安装，运行与测试均可断网。

```sh
python3 -m compileall -q partitionflow tests
python3 -m build --wheel --no-isolation
python3 -m unittest discover -s tests -v
python3 -m partitionflow
```

默认示例在独立临时目录执行，结束后删除目录。需要检查持久文件时可执行 `python3 -m partitionflow --root state`。测试自己创建临时目录，不依赖固定端口、外部服务或其他用例顺序。

## 按真实输入依赖补算

补报处理不再靠人工猜测重跑范围，也不靠清库重来。值班流程围绕“固定输入版本的一次运行（run）”展开：

1. `plan.build_plan` / `runner.create_run` 读取 `calendar.json` 的地区与时区，把目标分区及其受影响业务周内应到齐的分区，固定到计划时刻的最新源版本（pins）。尚未到齐的分区进入 `missing`，对应步骤标记 `blocked`，运行保持 `blocked` 而不是发布半成品。
2. `runner.advance_run` 只执行未成功的步骤。日、周结果按内容寻址且幂等，执行者中断后再次调用即从断点继续；失败步骤记录 `attempts` 与错误，已成功步骤不会重算。
3. `runner.switch_current` 在单事务内完成切换，并做三道检查：逐分区核对 pins 仍是最新源版本（否则 `StaleRunError`，运行标记 `superseded`，旧输入结果不能发布成最新）、再次核对业务日边界（backfill 永不覆盖当日批次）、全部步骤必须 `done`（下游继续读取上一套完整结果）。
4. 消费者只通过 `runner.read_current` / `current_snapshot` 读取 `current_entries`：明细、日汇总、周汇总在切换前后各自是自洽的一整套，快照会校验所有条目来自同一次切换（同一 `run_id` 与 `fence`）。
5. 迟到数据到齐后用 `runner.refresh_run` 重新固定版本并抬高 fence；仍持有旧计划的执行者在切换时会被拒绝。
6. `plan.explain_recompute` 说明某次补报为什么重算（首传、内容更正还是同内容重传），`plan.daily_lineage`、`weekly_lineage`、`release_lineage` 从输出或封存发布反查每个源分区版本及其是否已被超越。已命名的封存发布保持不可变。

两类运行边界明确：`backfill` 只接受各地区当前业务日之前的分区；`daily` 只接受当前业务日，且只切换当日条目，不被同周未来日阻塞，周汇总在周内分区到齐后由 backfill 统一切换。

端到端演示（含旧执行者被拒、刷新后一致切换、封存发布血缘）：

```sh
python3 -m partitionflow --scenario backfill
python3 -m partitionflow --scenario backfill --root state
```

## 容器

```sh
docker build -t partition-jobs:local .
docker run --rm --network none partition-jobs:local
docker run --rm --network none partition-jobs:local python3 -m partitionflow
```

容器构建负责预装锁定依赖，启动不下载包。示例输出是真实接入、查询和发布的数据，不读取预制输出。测试及示例可分别运行，退出码非零表示失败。

## 交换口径

进一步的字段含义、范围及兼容边界见 `docs/data-contract.md`。
