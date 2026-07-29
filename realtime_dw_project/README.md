# 实时数仓运行目录

完整项目说明见仓库根目录 [README](../README.md)。

## 本目录包含

- `docker-compose.yml`：6 个本地服务
- `flink-sql/`：ODS Source、DWD 清洗与维表关联、ADS 窗口聚合
- `scripts/`：初始化、事件生成、批量装载、验证、质量报告和告警
- `dashboard/`：Streamlit 看板
- `docs/`：架构、数据模型、面试问答和质量报告

## 运行

```bash
docker compose up -d
bash scripts/run_demo.sh
python3 -m streamlit run dashboard/app.py
```

> `run_demo.sh` 会重建容器和数据卷，只应在本地 Demo 环境运行。

## 核心口径

- PV 只统计 `view` 事件
- UV 统计发生 `view` 的去重用户
- 商品/品类表是窗口聚合，看板选择 Top 10
- 渠道表是阶段人数对比，不是严格用户路径漏斗
- DWS 是 Flink 逻辑聚合层，没有单独持久化
