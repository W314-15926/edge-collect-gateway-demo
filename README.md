# edge‑collect‑gateway‑demo
边缘侧 Modbus‑TCP PLC 数据采集网关

## 项目简介
工业边缘侧采集网关，采用多线程模型周期性读取Modbus‑TCP保持寄存器；
生产者‑消费者队列实现数据削峰，批量写入MySQL数据库。
支持设备熔断黑名单、采集限流、本机采集防并发锁、告警防抖、离线本地缓存容错；
对外提供FastAPI运维与查询接口，实现优雅停机。Redis为可选组件，Redis异常断开会自动降级使用内存兜底，不阻断主采集业务。

## 测试环境说明
> 本Demo**无真实工业PLC硬件**，开发与全部功能验证基于 Modbus‑Slave 模拟器模拟多台Modbus‑TCP从站设备完成。
> 用于学习工业边缘采集的并发、容错、边界异常处理，不是商用生产版本。

## 技术栈
Python3.9+ | FastAPI | uvicorn | pymodbus | PyMySQL | redis | pyyaml | threading / Queue

## 核心能力
- Modbus‑TCP：每个从站独立采集线程，内置Modbus自动重连逻辑
- 生产者‑消费者线程安全队列做缓冲，设置队列上限，队列满直接丢弃最新数据并打印告警日志，防止内存溢出
- 自定义MySQL连接池，增加连接有效性检测，自动处理数据库闪断、僵死失效连接
- Redis实现：设备熔断黑名单、ZSet滑动窗口采集限流、**本机采集防并发锁**；Redis不可用时自动降级内存黑名单兜底
- 业务容错：告警防抖、脏寄存器过滤、从站在线/离线状态判定
- 离线容错：MySQL不可用时写入本地JSON缓存；数据库恢复后自动回放缓存数据入库
- 日志：滚动文件日志，本地txt持久化采集记录、告警记录、统计数据
- FastAPI接口：健康检查、运行统计查询、告警黑名单运维、配置管理，附带慢请求监控中间件
- 优雅停机：捕获SIGINT / SIGTERM信号，等待正在执行的采集、入库任务完成后再退出程序

## 架构简述
1. **采集层**：每台从站分配独立采集线程；采集前做黑名单、限流、本机防并发锁校验，读取寄存器后投递至线程安全队列。
2. **队列缓冲层**：线程安全Queue作为缓冲，设置最大长度，防止消息堆积造成OOM。
3. **消费持久层**：消费线程（数量可配）从队列取出数据，批量组装写入MySQL；数据库故障时写入本地JSON离线缓存。
4. **控制层**：Redis实现熔断、限流、本机防并发锁；Redis不可用切换内存兜底方案。
5. **API层**：FastAPI提供运维、监控、数据查询HTTP接口。
6. **后台辅助线程**：内存监控、设备健康巡检、指标统计线程，独立运行不阻塞主业务。

## 项目目录简要
```tree
edge-collect-gateway-demo
├── src
│   └── main.py           # 程序主入口（采集、连接池、接口、工具均在本文件内）
├── app.yaml.example      # 配置模板，复制为 app.yaml 后修改（app.yaml 不入库）
├── schema.sql            # 建表 SQL（业务主表 + 历史备份表）
├── requirements.txt      # 项目依赖
├── bug_notes.md          # 4 类缺陷：现象、根因、改动点、复现、验证
├── history_iter/         # 历史迭代版本 v1–v3，仅留档不参与运行
├── collect.log           # 运行日志（256MB × 5 轮转，运行时生成）
├── offline_cache.json    # MySQL 不可用时的离线缓存（运行时生成）
├── normal_record.txt     # 采集明细记录（运行时生成）
├── alarm_record.txt      # 告警记录（运行时生成）
└── stat_record.txt       # 统计记录（运行时生成）
```

## 测试说明
功能验证：使用 Modbus‑Slave 模拟器构造正常数据、脏寄存器、设备断连重连等场景手动测试。
故障注入测试：手动关停 MySQL、Redis 服务，验证降级逻辑、离线缓存、内存兜底是否生效。
异常场景验证：程序 Ctrl+C 中断，验证 JSON 半写容错、优雅停机逻辑。

## RESTful API 接口说明
基于 FastAPI 实现一套运维监控接口，用于存活/就绪探测、设备统计查询、告警黑名单与熔断运维、配置读写，**不对外提供原始采集数据的写操作**。

> 服务监听 `0.0.0.0:8001`（不是 8000）。交互式文档地址：`http://127.0.0.1:8001/docs`，由 FastAPI 自动生成。

| 分类 | 方法与路径 | 说明 |
|---|---|---|
| 健康探测 | `GET /health/live` | 存活探针，只表示进程活着 |
| 健康探测 | `GET /health/ready` | 就绪探针：连库校验 + 队列背压，积压超过 800 返回 503 |
| 配置 | `GET /api/config` | 读取内存中的当前运行配置 |
| 配置 | `PUT /api/config` | 全量替换内存配置，**不落盘**，重启即失效 |
| 配置 | `POST /api/config/save` | 将内存配置写回 `app.yaml` |
| 设备统计 | `GET /api/devices/statistics` | 全部从站的最新温度/压力、成功与失败次数、在线状态 |
| 设备统计 | `GET /api/devices/{slave_id}/statistics` | 单个从站同上；从站不存在返回 404 |
| 告警黑名单 | `POST /api/alarms/blacklist` | 新增一条（从站 + 告警类型），201 Created |
| 告警黑名单 | `DELETE /api/alarms/blacklist/{slave_id}/types/{alarm_type}` | 删除指定条目 |
| 告警黑名单 | `GET /api/alarms/blacklist` | 查询全部告警黑名单 |
| 熔断黑名单 | `POST /api/devices/{slave_id}/blacklist` | 设备拉黑，TTL 300 秒 |
| 熔断黑名单 | `DELETE /api/devices/{slave_id}/blacklist` | 设备解除拉黑 |

## API附加特性
- 内置慢请求中间件，请求耗时超过阈值自动打印告警日志；
- 统一 JSON 返回结构 `{"code":0,"msg":"...","data":...}`，异常通过 `HTTPException` 返回 400 / 404 / 500 / 503；
- **未实现身份鉴权，且 `GET /api/config` 直接返回完整运行配置（含数据库账号密码）。只能单机或内网使用，禁止暴露公网；上生产必须补 Token 鉴权与敏感字段脱敏。**


## 快速启动
复制配置模板 `cp app.yaml.example app.yaml`，修改 Modbus、MySQL、Redis 连接配置
安装依赖
```bash 
pip install -r requirements.txt
```
执行 MySQL 建库，建表 SQL（已提供 `schema.sql`）：

### 建库（第一次部署才需要）

```sql
CREATE DATABASE IF NOT EXISTS modbus_db
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;
```
### 建表（第一次部署才需要）

```bash
mysql -u root -p modbus_db < schema.sql
```

运行程序（必须在项目根目录执行，程序按 `src` 的上一级定位 `app.yaml`，日志也写在当前工作目录）
```bash
python src/main.py
```

启动时先做自检：MySQL 连接失败或缺表会直接退出（退出码 1）；Modbus 连接测试失败只告警不退出。自检通过后依次启动采集、消费、统计、健康巡检、内存监控与接口线程。停止用 `Ctrl+C` 或 `kill -TERM`，程序等待在途任务完成后退出，最长 10 秒，不要用 `kill -9`。

## 项目说明
- 本项目为工业采集服务学习 Demo。
- Redis 锁仅用于**本机多线程防重复采集**，并未实现跨机器真正分布式锁（缺少锁续期、主从切换等逻辑）。
- 模拟环境下的故障修复记录、复现与验证方案查看 `bug_notes.md`。
- 批次 6：已完成全局大锁拆分为多把业务细粒度锁，降低锁竞争
- 批次 7：已实现 StateManager 统一收拢绝大多数业务状态
- 后续待优化：进一步缩小锁临界区；收拢剩余顶层运行时全局对象。

