# 按地区业务日运行的活动数仓作业

这是一个处理本地数据交换的仓湖项目。示例使用虚构机构和匿名合成记录，不连接业务平台。

## 数据契约

源分区键为地区与地区业务日期，每次不同内容产生新版本，相同内容重传复用旧版本。当前作业为显式指定输入的日汇总与周汇总，周为周一起始的业务周。发布名称绑定一组不可变作业结果，运行作业不自动覆盖发布。日界映射使用IANA时区，不接受无时区时间。

## 代码职责

- `partitionflow/calendar.py`：地区业务日、UTC边界与周归属。
- `partitionflow/engine.py`：显式输入的源、日与周结果。
- `partitionflow/publication.py`：固定输入版本的查询发布。
- `partitionflow/storage.py`：本地事务、内容摘要和查询工具。

## 安装与运行

使用 Python 3.11。先执行 `python3 -m pip install -r requirements.txt`；依赖仅在准备环境时安装，运行与测试均可断网。

```sh
python3 -m compileall -q partitionflow tests
python3 -m unittest discover -s tests -v
python3 -m partitionflow
```

默认示例在独立临时目录执行，结束后删除目录。需要检查持久文件时可执行 `python3 -m partitionflow --root state`。测试自己创建临时目录，不依赖固定端口、外部服务或其他用例顺序。

## 容器

```sh
docker build -t partition-jobs:local .
docker run --rm --network none partition-jobs:local
docker run --rm --network none partition-jobs:local python3 -m partitionflow
```

容器构建负责预装锁定依赖，启动不下载包。示例输出是真实接入、查询和发布的数据，不读取预制输出。测试及示例可分别运行，退出码非零表示失败。

## 交换口径

进一步的字段含义、范围及兼容边界见 `docs/data-contract.md`。
