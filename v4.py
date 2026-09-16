import time
import json
import os
import sys
import threading
import struct
import traceback
import logging
import uvicorn
import signal
import pymysql
import psutil
import yaml
import redis
from redis.exceptions import RedisError
from pymysql import OperationalError
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException
from typing import Dict, List, Optional, Any, Tuple
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from queue import Queue, Empty, Full
from logging.handlers import RotatingFileHandler
import uuid

# ===================== 全局统计/状态变量 =====================
collect_total = 0
collect_success = 0
collect_fail = 0
db_write_total = 0
db_write_fail = 0
queue_drop_cnt = 0
alarm_trigger_cnt = 0
alarm_time_map: Dict[str, float] = {}
slave_status_cache: Dict[int, Dict[str, Any]] = {}
stat_data_window: Dict[int, List[List[Optional[float]]]] = {}
alarm_blacklist: Dict[int, List[str]] = {}  # Bug1修复：补缺失全局告警黑名单
# 本地内存黑名单（Redis故障降级兜底）
local_blacklist: Dict[int, float] = {}

uvicorn_server: Optional[uvicorn.Server] = None
DATA_QUEUE: Optional[Queue[Dict[str, Any]]] = None
SHUTDOWN_FLAG = False
UVICORN_RUNNING = False
RUNTIME_CONFIG: Dict[str, Any] = {}
app = FastAPI(title="modbus采集查询服务 V4")

# ========== 批次6：细粒度拆分锁，移除单一GLOBAL_LOCK大锁 ==========
_lock_stats = threading.Lock()          # 采集统计计数器
_lock_slave_status = threading.Lock()   # slave_status_cache 从站状态
_lock_window = threading.Lock()         # stat_data_window 滑动窗口
_lock_alarm = threading.Lock()          # alarm_time_map / alarm_blacklist 告警
_lock_local_black = threading.Lock()    # local_blacklist redis降级内存黑名单
_lock_runtime_cfg = threading.Lock()    # RUNTIME_CONFIG配置读取
_lock_file_io = threading.Lock()        # txt文件写入（IO必须串行）
_lock_json_cache = threading.Lock()      # 离线json缓存读写

# ===================== Redis封装类 V4核心新增 =====================
class RedisClientWrap:
    def __init__(self, redis_cfg: dict):
        self.cfg = redis_cfg
        self.redis_client: Optional[redis.Redis] = None
        self._redis_available = True
        self.connect()

    def connect(self) -> None:
        try:
            self.redis_client = redis.Redis(
                host=self.cfg["host"],
                port=self.cfg["port"],
                password=self.cfg.get("password", ""),
                db=self.cfg.get("db", 0),
                socket_timeout=2
            )
            self.redis_client.ping()
            self._redis_available = True
            logger.info("Redis连接成功")
        except RedisError:
            self._redis_available = False
            logger.warning("Redis连接失败，自动降级本地内存黑名单")

    def is_redis_ok(self) -> bool:
        if not self._redis_available or self.redis_client is None:
            return False
        try:
            self.redis_client.ping()
            return True
        except RedisError:
            self._redis_available = False
            logger.warning("Redis掉线，切换本地内存兜底")
            return False

    # 分布式熔断黑名单 key: blacklist:{slave_id}，TTL 300s(5分钟)
    def add_blacklist(self, slave_id: int, ttl: int = 300) -> bool:
        if self.is_redis_ok():
            try:
                self.redis_client.setex(f"blacklist:{slave_id}", ttl, "1")
                return True
            except RedisError:
                pass
        # 降级写入内存，使用独立内存黑名单锁
        with _lock_local_black:
            local_blacklist[slave_id] = time.time() + ttl
        return True

    def in_blacklist(self, slave_id: int) -> bool:
        if self.is_redis_ok():
            try:
                res = self.redis_client.get(f"blacklist:{slave_id}")
                return res is not None
            except RedisError:
                pass
        # 读取本地内存黑名单
        with _lock_local_black:
            if slave_id in local_blacklist:
                if time.time() < local_blacklist[slave_id]:
                    return True
                else:
                    del local_blacklist[slave_id]
        return False

    def remove_blacklist(self, slave_id: int) -> bool:
        if self.is_redis_ok():
            try:
                self.redis_client.delete(f"blacklist:{slave_id}")
            except RedisError:
                pass
        with _lock_local_black:
            if slave_id in local_blacklist:
                del local_blacklist[slave_id]
        return True

    # ZSet滑动窗口限流：控制PLC采集频次
    def zset_rate_limit(self, slave_id: int, limit_cnt: int, window_seconds: int) -> bool:
        """
        return True:触发限流，禁止采集；False：允许采集
        """
        if not self.is_redis_ok():
            return False
        try:
            key = f"rate:zset:{slave_id}"
            now_ts = time.time()
            # 加入当前时间戳score
            self.redis_client.zadd(key, {uuid.uuid4().hex: now_ts})
            # 删除窗口外旧记录
            self.redis_client.zremrangebyscore(key, 0, now_ts - window_seconds)
            # 设置key过期
            self.redis_client.expire(key, window_seconds + 10)
            current_count: int = self.redis_client.zcard(key)
            if current_count > limit_cnt:
                return True
            return False
        except RedisError:
            return False

    # 分布式锁，采集前置抢锁，保证幂等，防止多实例重复采集
    def try_acquire_lock(self, lock_key: str, expire: int = 5) -> Optional[str]:
        if not self.is_redis_ok():
            return None
        lock_val = uuid.uuid4().hex
        try:
            ok: Optional[bool] = self.redis_client.set(lock_key, lock_val, nx=True, ex=expire)
            if ok:
                return lock_val
            return None
        except RedisError:
            return None

    def release_lock(self, lock_key: str, lock_val: str) -> None:
        if not self.is_redis_ok():
            return
        try:
            script = """
                if redis.call('get',KEYS[1]) == ARGV[1] then
                    return redis.call('del',KEYS[1])
                else
                    return 0
                end
            """
            self.redis_client.eval(script, 1, lock_key, lock_val)
        except RedisError:
            pass


redis_wrap: Optional[RedisClientWrap] = None

# ===================== Pydantic 请求模型 =====================
class ConfigReloadModel(BaseModel):
    slave_id: Optional[int] = Field(None, ge=1, description="从站id")
    stable_threshold: Optional[float] = Field(None, ge=0.1, le=50, description="稳定波动阈值")
    stat_interval: Optional[int] = Field(None, ge=5, description="统计定时间隔")


class BlacklistAddModel(BaseModel):
    slave_id: Optional[int] = Field(None, ge=1, description="从站id")
    alarm_type: Optional[str] = Field(None, description="告警类型")


class BlacklistRemoveModel(BaseModel):
    slave_id: Optional[int] = Field(None, ge=1, description="从站id")
    alarm_type: Optional[str] = Field(None, description="告警类型")

# ===================== 日志初始化（批次8：轮转参数合理化） =====================
def init_logger(log_name: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    log_format = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    # 批次8：单文件256MB，保留5个备份
    file_handler = RotatingFileHandler(log_file, maxBytes=256 * 1024 * 1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(log_format)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


logger = init_logger("collect", "collect.log")

# ===================== 信号处理 =====================
def handle_receive_signal(signum: int, frame: Optional[Any]) -> None:
    global SHUTDOWN_FLAG, UVICORN_RUNNING, uvicorn_server
    logger.info(f"[signal]收到退出信号 {signum}，准备优雅停机")
    SHUTDOWN_FLAG = True
    UVICORN_RUNNING = False
    if uvicorn_server is not None:
        uvicorn_server.should_exit = True


signal.signal(signal.SIGINT, handle_receive_signal)
signal.signal(signal.SIGTERM, handle_receive_signal)

# ===================== 文件工具函数 =====================
def init_txt(file_path: str) -> None:
    if os.path.exists(file_path):
        return
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            pass
        logger.info(f"创建文件 {file_path} 成功")
    except PermissionError:
        logger.warning(f"文件 {file_path} 权限不足")
    except Exception:
        logger.error(f"创建文件 {file_path} 失败\n{traceback.format_exc()}")


def write_to_normal_txt(collect_time: str, slave_id: int, reg_list: List[int], float_list: List[float], status: str) -> None:
    with _lock_runtime_cfg:
        file_path = RUNTIME_CONFIG["txt_normal_path"]
    line = f"time:{collect_time},slave_id:{slave_id},reg_list:{reg_list},float_list:{float_list},status:{status}\n"
    init_txt(file_path)
    # 文件IO使用独立文件锁
    with _lock_file_io:
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(line)
        except OSError:
            logger.warning(f"写入 {file_path} IO异常")
        except Exception:
            logger.error(f"写入普通txt失败\n{traceback.format_exc()}")


def write_to_alarm_txt(collect_time: str, slave_id: int, float_list: List[float], alarm_str: str, alarm_level: str) -> None:
    with _lock_runtime_cfg:
        file_path = RUNTIME_CONFIG["txt_alarm_path"]
    line = f"time:{collect_time},slave_id:{slave_id},float_list:{float_list},alarm_str:{alarm_str},alarm_level:{alarm_level}\n"
    init_txt(file_path)
    with _lock_file_io:
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(line)
        except OSError:
            logger.warning(f"写入 {file_path} IO异常")
        except Exception:
            logger.error(f"写入告警txt失败\n{traceback.format_exc()}")


def write_to_stat_txt(collect_time: str, slave_id: int, temp_list: List[float], press_list: List[float]) -> None:
    with _lock_runtime_cfg:
        file_path = RUNTIME_CONFIG["txt_stat_path"]
    init_txt(file_path)
    lines: List[str] = []
    if len(temp_list) > 0:
        t_max, t_min, t_ave = max(temp_list), min(temp_list), round(sum(temp_list) / len(temp_list), 2)
        lines.append(f"time:{collect_time},slave_id:{slave_id},temp_max:{t_max},temp_min:{t_min},temp_ave:{t_ave}\n")
    if len(press_list) > 0:
        p_max, p_min, p_ave = max(press_list), min(press_list), round(sum(press_list) / len(press_list), 2)
        lines.append(f"time:{collect_time},slave_id:{slave_id},press_max:{p_max},press_min:{p_min},press_ave:{p_ave}\n")
    with _lock_file_io:
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                f.writelines(lines)
        except OSError:
            logger.warning(f"写入 {file_path} IO异常")
        except Exception:
            logger.error(f"写入stat txt失败\n{traceback.format_exc()}")

# ===================== 从站上下线监控（批次5原有逻辑不动，更换锁） =====================
def slave_online_monitor(slave_id: int, success: bool) -> int:
    with _lock_slave_status:
        if slave_id not in slave_status_cache:
            slave_status_cache[slave_id] = {"success_cnt": 0, "fail_cnt": 0, "offline": 0}
        info = slave_status_cache[slave_id]
    with _lock_runtime_cfg:
        slave_online_cnt = RUNTIME_CONFIG["slave_online_cnt"]
        slave_offline_cnt = RUNTIME_CONFIG["slave_offline_cnt"]

    if success:
        with _lock_slave_status:
            info["success_cnt"] += 1
            info["fail_cnt"] = 0
            if info["offline"] == 1 and info["success_cnt"] >= slave_online_cnt:
                info["offline"] = 0
                logger.info(f"从站{slave_id}恢复上线")
    else:
        with _lock_slave_status:
            info["fail_cnt"] += 1
            info["success_cnt"] = 0
            if info["offline"] == 0 and info["fail_cnt"] >= slave_offline_cnt:
                info["offline"] = 1
                logger.info(f"从站{slave_id}下线")
        # V4新增：下线加入分布式黑名单，TTL5分钟
        if redis_wrap is not None:
            redis_wrap.add_blacklist(slave_id, ttl=300)
    with _lock_slave_status:
        return info["offline"]

# ===================== 滑动窗口稳定性判断 =====================
def stat_stable(window_data: List[List[Optional[float]]]) -> int:
    with _lock_runtime_cfg:
        stable_window_cnt = RUNTIME_CONFIG["stable_window_cnt"]
        stable_threshold = RUNTIME_CONFIG["stable_threshold"]
    if len(window_data) < stable_window_cnt:
        return 0
    recent = window_data[-stable_window_cnt:]
    temp_group = [item[0] for item in recent if len(item) >= 1 and item[0] is not None]
    press_group = [item[1] for item in recent if len(item) >= 1 and item[1] is not None]
    if len(temp_group) < stable_window_cnt or len(press_group) < stable_window_cnt:
        return 0
    temp_ok = all(abs(temp_group[i] - temp_group[j]) <= stable_threshold for i in range(stable_window_cnt) for j in range(i + 1, stable_window_cnt))
    press_ok = all(abs(press_group[i] - press_group[j]) <= stable_threshold for i in range(stable_window_cnt) for j in range(i + 1, stable_window_cnt))
    return 1 if (temp_ok or press_ok) else 0

# ===================== 脏寄存器过滤 批次5 Bug3修复保留不动 =====================
def filter_dirty_regs(reg_list: List[int]) -> bool:
    """
    返回True代表脏数据，丢弃；False代表正常数据
    Bug3修复：同时拦截0和65535脏寄存器
    """
    if len(reg_list) == 0:
        return True
    for r in reg_list:
        if r == 0 or r == 65535:
            logger.debug(f"检测脏寄存器值 {r}")
            return True
    return False

# ===================== struct解析寄存器转float =====================
def parse_regs_to_floats(reg_list: List[int]) -> List[float]:
    floats: List[float] = []
    if len(reg_list) < 2:
        return floats
    try:
        for i in range(0, len(reg_list), 2):
            if i + 1 >= len(reg_list):
                break
            r1 = reg_list[i]
            r2 = reg_list[i + 1]
            pack_buf = struct.pack(">HH", r1, r2)
            fv: float = struct.unpack(">f", pack_buf)[0]
            floats.append(round(fv, 2))
    except Exception:
        logger.error(f"寄存器解析失败\n{traceback.format_exc()}")
    return floats

# ===================== 告警防抖逻辑 =====================
def alarm_rule(slave_id: int, float_list: List[float]) -> Tuple[str, str]:
    if not float_list:
        return "", ""
    alarm_info: List[str] = []
    alarm_level = ""
    now_ts = time.time()
    collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    temp = float_list[0] if len(float_list) >= 1 else None
    press = float_list[1] if len(float_list) >= 2 else None
    with _lock_runtime_cfg:
        alarm_interval = RUNTIME_CONFIG["alarm_interval"]

    if temp is not None and temp > 50:
        key = f"{slave_id}_temp"
        with _lock_alarm:
            if key not in alarm_time_map or now_ts - alarm_time_map[key] > alarm_interval:
                alarm_info.append("温度过高")
                alarm_time_map[key] = now_ts
    if press is not None and press < 0:
        key = f"{slave_id}_press"
        with _lock_alarm:
            if key not in alarm_time_map or now_ts - alarm_time_map[key] > alarm_interval:
                alarm_info.append("压力过低")
                alarm_time_map[key] = now_ts
    alarm_str = "、".join(alarm_info)
    if "温度过高" in alarm_str:
        alarm_level = "一级警报"
    elif "压力过低" in alarm_str:
        alarm_level = "二级警报"
    if alarm_str:
        logger.info(f"{collect_time} 从站{slave_id} {alarm_str} {alarm_level}")
        write_to_alarm_txt(collect_time, slave_id, float_list, alarm_str, alarm_level)
        global alarm_trigger_cnt
        with _lock_stats:
            alarm_trigger_cnt += 1
    return alarm_str, alarm_level

# ===================== Modbus重连 =====================
def modbus_reconnect(client: ModbusTcpClient) -> bool:
    client.close()
    time.sleep(1)
    ok = client.connect()
    if ok:
        logger.info("Modbus重连成功")
    else:
        logger.error("Modbus重连失败")
    return ok


def read_modbus_regs(client: ModbusTcpClient, slave_id: int, modbus_addr: int, modbus_count: int) -> Tuple[str, List[int], List[float], str]:
    collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    reg_list: List[int] = []
    float_list: List[float] = []
    status = "fail"
    if not client.is_socket_open():
        modbus_reconnect(client)
    try:
        # pymodbus>=3.4 从站ID设置到client实例属性，不能作为read函数参数
        client.unit = slave_id
        rsp = client.read_holding_registers(address=modbus_addr, count=modbus_count)
        if rsp.isError():
            return collect_time, reg_list, float_list, status
        reg_list = rsp.registers
        float_list = parse_regs_to_floats(reg_list)
        status = "success"
        logger.info(f"{collect_time} slave:{slave_id} reg:{reg_list}")
    except ModbusException:
        logger.warning("Modbus协议异常")
    except OSError:
        logger.warning("Modbus底层OS异常")
    except Exception:
        logger.error(f"读寄存器异常\n{traceback.format_exc()}")
    return collect_time, reg_list, float_list, status

# ===================== Mysql连接池 Bug2修复保留不动 =====================
class MysqlPool:
    def __init__(self, cfg: Dict[str, Any], max_idle: int):
        self.cfg = cfg
        self.max_idle = max_idle
        self.queue: Queue[pymysql.connections.Connection] = Queue(maxsize=self.max_idle)
        for _ in range(max_idle):
            try:
                conn = pymysql.connect(**cfg)
                self.queue.put(conn)
            except Exception:
                logger.error("初始化连接池创建连接失败")

    def get_conn(self) -> Optional[pymysql.connections.Connection]:
        try:
            conn: Optional[pymysql.connections.Connection]
            if not self.queue.empty():
                conn = self.queue.get()
            else:
                conn = pymysql.connect(**self.cfg)
            # 校验连接是否存活，断线自动重连
            conn.ping(reconnect=True)
            return conn
        except Exception:
            logger.error(f"获取连接失败\n{traceback.format_exc()}")
            return None

    def release_conn(self, conn: Optional[pymysql.connections.Connection]) -> None:
        if conn is None:
            return
        try:
            # 检查连接有效才放回池，失效直接关闭丢弃
            conn.ping(reconnect=False)
            if self.queue.qsize() < self.max_idle:
                self.queue.put(conn)
            else:
                conn.close()
        except (OperationalError, pymysql.err.InterfaceError):
            logger.warning("MySQL连接已失效，直接关闭丢弃，不归还连接池")
            try:
                conn.close()
            except Exception:
                pass
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    def close_all(self) -> None:
        while not self.queue.empty():
            c = self.queue.get()
            try:
                c.close()
            except Exception:
                pass

# ===================== SQL写入 =====================
def write_to_sql(pool: MysqlPool, batch_list: List[tuple]) -> bool:
    if not batch_list:
        return True
    conn = pool.get_conn()
    if conn is None:
        return False
    cur = None
    insert_sql = """
    INSERT INTO modbus_day2_data
    (collect_time,slave_id,reg_list,float_list,temperature,pressure,alarm_str,alarm_level,offline,status,stable)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    try:
        cur = conn.cursor()
        cur.executemany(insert_sql, batch_list)
        conn.commit()
        logger.info(f"批量入库成功 {len(batch_list)}条")
        return True
    except OperationalError:
        logger.warning("MySQL连接异常")
        conn.rollback()
        return False
    except Exception:
        logger.error(f"入库失败\n{traceback.format_exc()}")
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        pool.release_conn(conn)

# ===================== JSON离线缓存 Bug4修复保留不动，更换独立锁 =====================
def save_to_json(batch_list: list) -> None:
    if not batch_list:
        return
    with _lock_runtime_cfg:
        cache_file = RUNTIME_CONFIG["cache_file"]
        tmp_file = cache_file + ".tmp"
    cache_list = []
    try:
        if os.path.exists(cache_file):
            with _lock_json_cache:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cache_list = json.load(f)
    except json.JSONDecodeError:
        logger.warning("缓存文件损坏，本次只追加新数据")
    except Exception:
        logger.error(f"读缓存失败\n{traceback.format_exc()}")
    cache_list.extend(batch_list)
    try:
        with _lock_json_cache:
            # 先写临时文件，成功再替换原文件，避免半写损坏
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(cache_list, f, ensure_ascii=False)
            os.replace(tmp_file, cache_file)
    except Exception:
        logger.error(f"写离线缓存失败，内存保留这批数据，等待下一轮重试\n{traceback.format_exc()}")


def load_and_replay_to_sql(pool: MysqlPool) -> None:
    with _lock_runtime_cfg:
        cache_file = RUNTIME_CONFIG["cache_file"]
    if not os.path.exists(cache_file):
        return
    try:
        with _lock_json_cache:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache_data = json.load(f)
    except json.JSONDecodeError:
        logger.warning("缓存文件损坏，跳过回放")
        return
    except Exception:
        logger.error(f"加载缓存异常\n{traceback.format_exc()}")
        return
    if cache_data and isinstance(cache_data, list):
        ok = write_to_sql(pool, cache_data)
        if ok:
            logger.info("离线缓存回放入库完成")
            try:
                os.remove(cache_file)
            except OSError:
                logger.warning("缓存文件删除失败，可能被其他进程占用")

# ===================== 配置加载 yaml优先 =====================
def load_runtime_config() -> Dict[str, Any]:
    cfg: Dict[str, Any] = {}
    yaml_path = "app.yaml"
    if os.path.exists(yaml_path):
        try:
            with open(yaml_path, "r", encoding="utf-8") as f:
                yml_data = yaml.safe_load(f)
                if isinstance(yml_data, dict):
                    cfg.update(yml_data)
            logger.info("已加载 app.yaml")
            return cfg
        except Exception:
            logger.warning("app.yaml读取失败")
    logger.fatal("缺少app.yaml配置文件！")
    sys.exit(1)


def config_validate(cfg: dict) -> None:
    required_keys = [
        "txt_normal_path", "txt_alarm_path", "txt_stat_path", "cache_file",
        "stable_window_cnt", "slave_online_cnt", "slave_offline_cnt",
        "data_queue_maxsize", "consumer_thread_num", "max_thread_retry",
        "shutdown_max_wait", "stable_threshold", "alarm_interval",
        "stat_interval", "heartbeat_interval", "batch_data_max",
        "mem_warn_threshold_mb", "mysql", "modbus", "slave_dict", "redis"
    ]
    missing = []
    for k in required_keys:
        if k not in cfg:
            missing.append(k)
    if missing:
        logger.fatal(f"配置缺失顶层key: {missing}，请检查app.yaml")
        sys.exit(1)
    # 校验modbus子配置
    modbus_sub_keys = ["host", "port", "timeout", "addr", "count"]
    modbus_cfg = cfg["modbus"]
    sub_miss = []
    for sk in modbus_sub_keys:
        if sk not in modbus_cfg:
            sub_miss.append(sk)
    if sub_miss:
        logger.fatal(f"modbus配置缺失子key:{sub_miss}")
        sys.exit(1)


def save_config_to_yaml(cfg: dict) -> bool:
    tmp_name = "app.yaml.tmp"
    try:
        with open(tmp_name, "w", encoding="utf-8") as f:
            yaml.dump(cfg, f, allow_unicode=True, sort_keys=False)
        os.replace(tmp_name, "app.yaml")
        logger.info("配置写入app.yaml成功")
        return True
    except Exception:
        logger.error(f"写yaml失败\n{traceback.format_exc()}")
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        return False

# ===================== 程序启动自检 =====================
def startup_self_check(mysql_cfg: dict, modbus_cfg: dict) -> None:
    logger.info("===== 开始程序启动自检 =====")
    try:
        conn = pymysql.connect(**mysql_cfg)
        cur = conn.cursor()
        cur.execute("SHOW TABLES LIKE 'modbus_day2_data'")
        t1 = cur.fetchone()
        cur.execute("SHOW TABLES LIKE 'modbus_history_backup'")
        t2 = cur.fetchone()
        cur.close()
        conn.close()
        if not t1 or not t2:
            logger.fatal("自检失败：业务数据表缺失，请执行建表SQL")
            sys.exit(1)
    except Exception:
        logger.fatal("自检失败：MySQL无法连接")
        sys.exit(1)
    c = ModbusTcpClient(host=modbus_cfg["host"], port=modbus_cfg["port"], timeout=modbus_cfg["timeout"])
    if not c.connect():
        logger.warning("ModbusTCP连接测试失败；如果没有仿真PLC可以忽略此警告")
    c.close()
    with _lock_runtime_cfg:
        fp_list = [
            RUNTIME_CONFIG["txt_normal_path"],
            RUNTIME_CONFIG["txt_alarm_path"],
            RUNTIME_CONFIG["txt_stat_path"],
            RUNTIME_CONFIG["cache_file"]
        ]
    for fp in fp_list:
        init_txt(fp)
    logger.info("===== 全部自检通过，启动业务线程 =====")

# ===================== 内存监控线程 =====================
def mem_monitor_thread() -> None:
    proc = psutil.Process(os.getpid())
    while not SHUTDOWN_FLAG:
        try:
            mem_info = proc.memory_info()
            rss_mb = mem_info.rss / 1024 / 1024
            with _lock_runtime_cfg:
                mem_thresh = RUNTIME_CONFIG["mem_warn_threshold_mb"]
                if rss_mb > mem_thresh:
                    logger.warning(f"内存告警 当前RSS={rss_mb:.2f}MB 阈值={mem_thresh}MB")
            time.sleep(10)
        except Exception:
            time.sleep(2)
    logger.info("内存监控线程退出")

# ===================== 采集线程内层循环 批次5业务完全不动；批次6更换细粒度锁 =====================
def _inner_collect_loop(slave_id: int, interval: int, modbus_cfg: Dict[str, Any]) -> None:
    # ✅每个采集线程私有ModbusTcpClient，规避多线程共享client非线程安全问题
    client = ModbusTcpClient(host=modbus_cfg["host"], port=modbus_cfg["port"], timeout=modbus_cfg["timeout"])
    if not client.is_socket_open():
        modbus_reconnect(client)
    with _lock_window:
        stat_data_window[slave_id] = []
    while not SHUTDOWN_FLAG:
        start_ts = time.time()
        # ========== V4新增核心逻辑 START ==========
        # 1.先判断分布式黑名单，如果命中，跳过本次采集
        if redis_wrap is not None and redis_wrap.in_blacklist(slave_id):
            logger.debug(f"从站{slave_id}在熔断黑名单，跳过采集")
            time.sleep(1)
            continue
        #2.ZSet滑动窗口限流，10秒最多采集5次
        if redis_wrap is not None and redis_wrap.zset_rate_limit(slave_id, limit_cnt=5, window_seconds=10):
            logger.warning(f"从站{slave_id}触发采集限流")
            time.sleep(0.5)
            continue
        #3.分布式锁，防止多实例同时采集同一个从站
        lock_key = f"collect:lock:{slave_id}"
        lock_val: Optional[str] = None
        if redis_wrap is not None:
            lock_val = redis_wrap.try_acquire_lock(lock_key, expire=5)
        if lock_val is None:
            logger.debug(f"从站{slave_id}获取采集锁失败，跳过本次采集")
            time.sleep(0.2)
            continue
        # ========== V4新增核心逻辑 END ==========
        ct, regs, floats, status = read_modbus_regs(client, slave_id, modbus_cfg["addr"], modbus_cfg["count"])
        success_flag = False
        with _lock_stats:
            global collect_total
            collect_total += 1
        if status == "success":
            if not filter_dirty_regs(regs):
                success_flag = True
                with _lock_stats:
                    global collect_success
                    collect_success += 1
                with _lock_window:
                    stat_data_window[slave_id].append(floats)
                    with _lock_runtime_cfg:
                        stable_window_cnt = RUNTIME_CONFIG["stable_window_cnt"]
                    if len(stat_data_window[slave_id]) > stable_window_cnt * 3:
                        stat_data_window[slave_id].pop(0)
                # 窗口拷贝已经在锁内完成，释放锁再做计算
                with _lock_window:
                    win_copy = stat_data_window[slave_id].copy()
                stable = stat_stable(win_copy)
                alarm_str, alarm_level = alarm_rule(slave_id, floats)
                offline = slave_online_monitor(slave_id, success_flag)
                write_to_normal_txt(ct, slave_id, regs, floats, status)
                temp = floats[0] if len(floats) >= 1 else None
                press = floats[1] if len(floats) >= 2 else None
                payload: Dict[str, Any] = {
                    "slave_id": slave_id, "collect_time": ct, "regs": regs, "float_list": floats,
                    "temp": temp, "press": press, "alarm_str": alarm_str,
                    "alarm_level": alarm_level, "stable": stable, "offline": offline
                }
                try:
                    assert DATA_QUEUE is not None
                    DATA_QUEUE.put(payload, timeout=0.1)
                except Full:
                    logger.error(f"队列满丢弃数据 slave_id:{slave_id}")
                    with _lock_stats:
                        global collect_fail
                        collect_fail += 1
            else:
                # Bug3修复：脏寄存器，**不执行下线判定**，只打日志，避免误判设备离线
                logger.debug(f"从站{slave_id}收到脏寄存器，不更新设备上下线状态")
        else:
            # modbus读取真正通信失败才更新下线状态
            slave_online_monitor(slave_id, success_flag)
        #释放分布式锁
        if redis_wrap is not None and lock_val is not None:
            redis_wrap.release_lock(lock_key, lock_val)
        cost = time.time() - start_ts
        sleep_t = max(0.01, interval - cost)
        time.sleep(sleep_t)
    client.close()
    logger.info(f"采集线程 slave:{slave_id}正常退出")


def slave_collect_thread_wrapped(slave_id: int, interval: int, modbus_cfg: Dict[str, Any]) -> None:
    with _lock_runtime_cfg:
        max_thread_retry = RUNTIME_CONFIG["max_thread_retry"]
    retry_cnt = 0
    while not SHUTDOWN_FLAG:
        try:
            _inner_collect_loop(slave_id, interval, modbus_cfg)
            break
        except Exception:
            retry_cnt += 1
            logger.error(f"采集线程slave{slave_id}异常崩溃 retry={retry_cnt}\n{traceback.format_exc()}")
            if retry_cnt >= max_thread_retry:
                logger.fatal(f"slave{slave_id}超过最大重启次数，不再重启")
                break
            time.sleep(2)

# ===================== 消费线程 =====================
def _inner_consumer_loop(pool: MysqlPool) -> None:
    batch_buffer: List[tuple] = []
    while not SHUTDOWN_FLAG or not DATA_QUEUE.empty():
        try:
            assert DATA_QUEUE is not None
            item = DATA_QUEUE.get(timeout=0.5)
            regs = item.get("regs", [])
            float_list = item.get("float_list", [])
            if len(regs) < 4:
                continue
            row_tuple = (
                item["collect_time"], item["slave_id"], json.dumps(regs), json.dumps(float_list),
                item["temp"], item["press"], item["alarm_str"], item["alarm_level"],
                item["offline"], "success", item["stable"]
            )
            batch_buffer.append(row_tuple)
            with _lock_runtime_cfg:
                batch_max = RUNTIME_CONFIG["batch_data_max"]
            if len(batch_buffer) >= batch_max:
                batch_copy = batch_buffer.copy()
                batch_buffer.clear()
                ok = write_to_sql(pool, batch_copy)
                #入库失败则写入离线缓存，保证不丢
                if not ok:
                    save_to_json(batch_copy)
        except Empty:
            continue
        except OSError:
            logger.warning("消费线程OS异常")
        except Exception:
            logger.error(f"消费循环异常\n{traceback.format_exc()}")
    if batch_buffer:
        save_to_json(batch_buffer)
    logger.info("消费内层循环退出")


def consumer_thread_wrapped(pool: MysqlPool) -> None:
    with _lock_runtime_cfg:
        max_thread_retry = RUNTIME_CONFIG["max_thread_retry"]
    retry_cnt = 0
    while not SHUTDOWN_FLAG:
        try:
            _inner_consumer_loop(pool)
            break
        except Exception:
            retry_cnt += 1
            logger.error(f"消费线程崩溃 retry={retry_cnt}\n{traceback.format_exc()}")
            if retry_cnt >= max_thread_retry:
                logger.fatal("消费线程超过最大重启次数，停止重启")
                break
            time.sleep(2)

# ===================== 统计线程 =====================
def stat_thread() -> None:
    logger.info("stat线程启动")
    while not SHUTDOWN_FLAG:
        start_ts = time.time()
        ct = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with _lock_runtime_cfg:
            stat_interval = RUNTIME_CONFIG["stat_interval"]
        with _lock_window:
            win_copy = stat_data_window.copy()
        for sid, rec in win_copy.items():
            t_list = [x[0] for x in rec if len(x) >= 1 and x[0] is not None]
            p_list = [x[1] for x in rec if len(x) >= 1 and x[1] is not None]
            write_to_stat_txt(ct, sid, t_list, p_list)
        cost = time.time() - start_ts
        sleep_t = max(0.01, stat_interval - cost)
        sleep_t = max(0.01, stat_interval - cost)
        step = 0.2
        while sleep_t > 0 and not SHUTDOWN_FLAG:
            t_slp = min(step, sleep_t)
            time.sleep(t_slp)
            sleep_t -= t_slp
    logger.info("stat线程退出")


def health_thread() -> None:
    logger.info("health线程启动")
    while not SHUTDOWN_FLAG:
        with _lock_runtime_cfg:
            hb_int = RUNTIME_CONFIG["heartbeat_interval"]
        time.sleep(hb_int)
        assert DATA_QUEUE is not None
        q_size = DATA_QUEUE.qsize()
        with _lock_slave_status:
            status_copy = slave_status_cache.copy()
        logger.info(f"[health]队列待处理:{q_size}")
        for sid, info in status_copy.items():
            st = "在线" if info["offline"] == 0 else "离线"
            logger.info(f"  slave{sid} {st} succ:{info['success_cnt']} fail:{info['fail_cnt']}")
    logger.info("health线程退出")

# ===================== FastAPI服务启动，端口改为8001解决占用 =====================
def run_app_cfg() -> None:
    global uvicorn_server, UVICORN_RUNNING
    UVICORN_RUNNING = True
    cfg = uvicorn.Config(app, host="0.0.0.0", port=8001, access_log=False, log_level="info")
    uvicorn_server = uvicorn.Server(cfg)
    uvicorn_server.run()
    UVICORN_RUNNING = False

# ===================== FastAPI中间件、接口 批次5 Bug1修复保留，更换锁 =====================
@app.middleware("http")
async def slow_request_middleware(request: Request, call_next):
    t0 = time.time()
    resp = await call_next(request)
    cost_ms = (time.time() - t0) * 1000
    if cost_ms > 500:
        client_ip = request.client.host if request.client else "unknown"
        logger.warning(f"慢请求警告 path={request.url.path} cost={cost_ms:.1f}ms ip={client_ip}")
    return resp


@app.get("/health/live")
async def health_live():
    return {"code": 0, "msg": "live"}


@app.get("/health/ready")
async def health_ready():
    assert DATA_QUEUE is not None
    q_size = DATA_QUEUE.qsize()
    with _lock_runtime_cfg:
        mysql_cfg = RUNTIME_CONFIG["mysql"]
    try:
        conn = pymysql.connect(**mysql_cfg)
        conn.close()
    except Exception:
        return {"code": -1, "msg": "db not ready", "data": None}
    if q_size > 800:
        return {"code": -2, "msg": "queue backpressure", "data": {"queue_size": q_size}}
    return {"code": 0, "msg": "ready", "data": {"queue_size": q_size}}


@app.post("/api/config/save_yaml")
async def api_save_yaml():
    ok = save_config_to_yaml(RUNTIME_CONFIG)
    if ok:
        return {"code": 0, "msg": "save yaml ok"}
    return {"code": -1, "msg": "save yaml fail"}


@app.get("/api/config/sta")
def api_config_sta():
    try:
        with _lock_runtime_cfg:
            d = RUNTIME_CONFIG.copy()
        return {"code": 0, "msg": "ok", "data": d}
    except Exception:
        return {"code": -1, "msg": "exception", "data": None}


@app.post("/api/alarm/blacklist/add")
def api_blacklist_add(req: BlacklistAddModel):
    try:
        sid = req.slave_id
        at = req.alarm_type
        with _lock_alarm:
            if sid not in alarm_blacklist:
                alarm_blacklist[sid] = []
            if at and at not in alarm_blacklist[sid]:
                alarm_blacklist[sid].append(at)
            return {"code": 0, "msg": "ok", "data": alarm_blacklist.copy()}
    except Exception:
        return {"code": -1, "msg": "err", "data": None}


@app.post("/api/alarm/blacklist/remove")
def api_blacklist_remove(req: BlacklistRemoveModel):
    try:
        sid = req.slave_id
        at = req.alarm_type
        with _lock_alarm:
            if sid in alarm_blacklist and at in alarm_blacklist[sid]:
                alarm_blacklist[sid].remove(at)
            return {"code": 0, "msg": "ok", "data": alarm_blacklist.copy()}
    except Exception:
        return {"code": -1, "msg": "err", "data": None}


@app.get("/api/alarm/blacklist/sta")
def api_blacklist_sta():
    with _lock_alarm:
        return {"code": 0, "msg": "ok", "data": alarm_blacklist.copy()}


@app.post("/api/blacklist/manual_add")
def api_blacklist_manual_add(slave_id: int = Query(..., ge=1), ttl: int = Query(300, ge=10)):
    """V4新增运维接口：手动加入分布式熔断黑名单"""
    if redis_wrap is not None:
        redis_wrap.add_blacklist(slave_id, ttl)
    return {"code": 0, "msg": f"slave {slave_id} 加入黑名单，ttl={ttl}s"}


@app.post("/api/blacklist/manual_remove")
def api_blacklist_manual_remove(slave_id: int = Query(..., ge=1)):
    """V4新增运维接口：手动移除分布式熔断黑名单"""
    if redis_wrap is not None:
        redis_wrap.remove_blacklist(slave_id)
    return {"code": 0, "msg": f"slave {slave_id} 移出黑名单"}


@app.get("/api/stat")
def api_stat(slave_id: int = Query(..., ge=1), limit: int = Query(50, ge=10, le=100)):
    try:
        with _lock_runtime_cfg:
            slave_dict = RUNTIME_CONFIG["slave_dict"]
        if slave_id not in slave_dict:
            return {"code": -1, "msg": "slave not exist", "data": []}
        with _lock_window:
            win_copy = stat_data_window.copy()
        with _lock_slave_status:
            st_copy = slave_status_cache.copy()
        rec = st_copy.get(slave_id, {})
        last_temp, last_press = None, None
        if slave_id in win_copy and len(win_copy[slave_id]) > 0:
            last_item = win_copy[slave_id][-1]
            last_temp = last_item[0] if len(last_item) >= 1 else None
            last_press = last_item[1] if len(last_item) >= 1 else None
        ret = {
            "slave_id": slave_id,
            "temp": last_temp,
            "press": last_press,
            "success_cnt": rec.get("success_cnt", 0),
            "fail_cnt": rec.get("fail_cnt", 0),
            "offline": rec.get("offline", 0)
        }
        return {"code": 0, "msg": "ok", "data": ret}
    except Exception:
        return {"code": -1, "msg": "err", "data": None}

# ===================== main入口 =====================
def main() -> None:
    global RUNTIME_CONFIG, DATA_QUEUE, redis_wrap
    RUNTIME_CONFIG = load_runtime_config()
    config_validate(RUNTIME_CONFIG)
    mysql_cfg = RUNTIME_CONFIG["mysql"]
    modbus_cfg = RUNTIME_CONFIG["modbus"]
    redis_cfg = RUNTIME_CONFIG["redis"]
    # V4初始化Redis包装类
    redis_wrap = RedisClientWrap(redis_cfg)
    # 配置加载完成后，再实例化队列
    DATA_QUEUE = Queue(maxsize=RUNTIME_CONFIG["data_queue_maxsize"])
    startup_self_check(mysql_cfg, modbus_cfg)
    consumer_num = RUNTIME_CONFIG["consumer_thread_num"]
    max_idle_pool = 2
    pool = MysqlPool(mysql_cfg, max_idle_pool)
    load_and_replay_to_sql(pool)
    thread_list: List[threading.Thread] = []
    for _ in range(consumer_num):
        t = threading.Thread(target=consumer_thread_wrapped, args=(pool,), daemon=False)
        thread_list.append(t)
        t.start()
    slave_dict = RUNTIME_CONFIG["slave_dict"]
    for sid, interval in slave_dict.items():
        t = threading.Thread(target=slave_collect_thread_wrapped, args=(sid, interval, modbus_cfg,), daemon=False)
        thread_list.append(t)
        t.start()
    t_stat = threading.Thread(target=stat_thread, daemon=False)
    thread_list.append(t_stat)
    t_stat.start()
    t_health = threading.Thread(target=health_thread, daemon=False)
    thread_list.append(t_health)
    t_health.start()
    t_mem = threading.Thread(target=mem_monitor_thread, daemon=False)
    thread_list.append(t_mem)
    t_mem.start()
    t_fapi = threading.Thread(target=run_app_cfg, daemon=False)
    thread_list.append(t_fapi)
    t_fapi.start()
    while not SHUTDOWN_FLAG:
        for t in thread_list:
            t.join(timeout=1.0)
    shutdown_wait = RUNTIME_CONFIG["shutdown_max_wait"]
    logger.info(f"开始等待全部线程退出，最大等待 {shutdown_wait}s")
    join_deadline = time.time() + shutdown_wait
    for t in thread_list:
        remain = join_deadline - time.time()
        if remain <= 0:
            logger.warning("达到停机超时，强制结束程序")
            break
        logger.info(f"等待线程 {t.name} 退出")
        t.join(timeout=remain)
    pool.close_all()
    logger.info("程序全部退出完成")


if __name__ == "__main__":
    main()
