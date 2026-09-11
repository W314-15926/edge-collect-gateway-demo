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
from pymysql import OperationalError
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from queue import Queue, Empty, Full
from logging.handlers import RotatingFileHandler

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
alarm_blacklist: Dict[int, List[str]] = {}
uvicorn_server: Optional[uvicorn.Server] = None
DATA_QUEUE: Optional[Queue[Dict]] = None
SHUTDOWN_FLAG = False
UVICORN_RUNNING = False
RUNTIME_CONFIG: Dict[str, Any] = {}
app = FastAPI(title="modbus采集查询服务")
GLOBAL_LOCK = threading.Lock()

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

# ===================== 日志初始化 =====================
def init_logger(log_name, log_file) -> logging.Logger:
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    log_format = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = RotatingFileHandler(log_file, maxBytes=512*1024*1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(log_format)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = init_logger("collect", "collect.log")

# ===================== 信号处理 =====================
def handle_receive_signal(signum, frame):
    global SHUTDOWN_FLAG, UVICORN_RUNNING, uvicorn_server
    logger.info(f"[signal]收到退出信号 {signum}，准备优雅停机")
    SHUTDOWN_FLAG = True
    UVICORN_RUNNING = False
    if uvicorn_server is not None:
        uvicorn_server.should_exit = True

signal.signal(signal.SIGINT, handle_receive_signal)
signal.signal(signal.SIGTERM, handle_receive_signal)

# ===================== 文件工具函数 =====================
def init_txt(file_path: str):
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

def write_to_normal_txt(collect_time, slave_id: int, reg_list: List, float_list: List, status: str):
    with GLOBAL_LOCK:
        file_path = RUNTIME_CONFIG["txt_normal_path"]
    line = f"time:{collect_time},slave_id:{slave_id},reg_list:{reg_list},float_list:{float_list},status:{status}\n"
    init_txt(file_path)
    try:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        logger.warning(f"写入 {file_path} IO异常")
    except Exception:
        logger.error(f"写入普通txt失败\n{traceback.format_exc()}")

def write_to_alarm_txt(collect_time, slave_id: int, float_list: List, alarm_str: str, alarm_level: str):
    with GLOBAL_LOCK:
        file_path = RUNTIME_CONFIG["txt_alarm_path"]
    line = f"time:{collect_time},slave_id:{slave_id},float_list:{float_list},alarm_str:{alarm_str},alarm_level:{alarm_level}\n"
    init_txt(file_path)
    try:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        logger.warning(f"写入 {file_path} IO异常")
    except Exception:
        logger.error(f"写入告警txt失败\n{traceback.format_exc()}")

def write_to_stat_txt(collect_time, slave_id: int, temp_list: List, press_list: List):
    with GLOBAL_LOCK:
        file_path = RUNTIME_CONFIG["txt_stat_path"]
    init_txt(file_path)
    lines = []
    if len(temp_list) > 0:
        t_max, t_min, t_ave = max(temp_list), min(temp_list), round(sum(temp_list)/len(temp_list),2)
        lines.append(f"time:{collect_time},slave_id:{slave_id},temp_max:{t_max},temp_min:{t_min},temp_ave:{t_ave}\n")
    if len(press_list) > 0:
        p_max, p_min, p_ave = max(press_list), min(press_list), round(sum(press_list)/len(press_list),2)
        lines.append(f"time:{collect_time},slave_id:{slave_id},press_max:{p_max},press_min:{p_min},press_ave:{p_ave}\n")
    try:
        with open(file_path, "a", encoding="utf-8") as f:
            f.writelines(lines)
    except OSError:
        logger.warning(f"写入 {file_path} IO异常")
    except Exception:
        logger.error(f"写入stat txt失败\n{traceback.format_exc()}")

# ===================== 从站上下线监控 =====================
def slave_online_monitor(slave_id:int, success:bool) -> int:
    if slave_id not in slave_status_cache:
        with GLOBAL_LOCK:
            slave_status_cache[slave_id] = {"success_cnt":0,"fail_cnt":0,"offline":0}
    info = slave_status_cache[slave_id]
    with GLOBAL_LOCK:
        slave_online_cnt = RUNTIME_CONFIG["slave_online_cnt"]
        slave_offline_cnt = RUNTIME_CONFIG["slave_offline_cnt"]
    if success:
        info["success_cnt"] +=1
        info["fail_cnt"] =0
        if info["offline"] ==1 and info["success_cnt"] >= slave_online_cnt:
            info["offline"] =0
            logger.info(f"从站{slave_id}恢复上线")
    else:
        info["fail_cnt"] +=1
        info["success_cnt"] =0
        if info["offline"] ==0 and info["fail_cnt"] >= slave_offline_cnt:
            info["offline"] =1
            logger.info(f"从站{slave_id}下线")
    return info["offline"]

# ===================== 滑动窗口稳定性判断 =====================
def stat_stable(window_data:List[List[Optional[float]]]) -> int:
    with GLOBAL_LOCK:
        stable_window_cnt = RUNTIME_CONFIG["stable_window_cnt"]
        stable_threshold = RUNTIME_CONFIG["stable_threshold"]
    if len(window_data) < stable_window_cnt:
        return 0
    recent = window_data[-stable_window_cnt:]
    temp_group = [item[0] for item in recent if len(item)>=1 and item[0] is not None]
    press_group = [item[1] for item in recent if len(item)>=1 and item[1] is not None]
    if len(temp_group) < stable_window_cnt or len(press_group) < stable_window_cnt:
        return 0
    t1,t2,t3 = temp_group[0],temp_group[1],temp_group[2]
    p1,p2,p3 = press_group[0],press_group[1],press_group[2]
    temp_ok = abs(t1-t2) <= stable_threshold and abs(t1-t3) <= stable_threshold and abs(t2-t3) <= stable_threshold
    press_ok = abs(p1-p2) <= stable_threshold and abs(p1-p3) <= stable_threshold and abs(p2-p3) <= stable_threshold
    return 1 if (temp_ok or press_ok) else 0

# ===================== 脏寄存器过滤 =====================
def filter_dirty_regs(reg_list):
    """
    返回True代表脏数据，丢弃；False代表正常数据
    注意：仿真器默认全0会被判定脏，真实设备按需修改此逻辑
    """
    if len(reg_list)==0:
        return True
    for r in reg_list:
        # if r ==0 or r ==65535:
        if r == 65535:
            logger.debug(f"检测脏寄存器 {r}")
            return True
    return False

# ===================== struct解析寄存器转float =====================
def parse_regs_to_floats(reg_list):
    floats = []
    if len(reg_list) <2:
        return floats
    try:
        for i in range(0, len(reg_list),2):
            if i+1 >= len(reg_list):
                break
            r1 = reg_list[i]
            r2 = reg_list[i+1]
            pack_buf = struct.pack(">HH", r1, r2)
            fv = struct.unpack(">f", pack_buf)[0]
            floats.append(round(fv,2))
    except Exception:
        logger.error(f"寄存器解析失败\n{traceback.format_exc()}")
    return floats

# ===================== 告警防抖逻辑 =====================
def alarm_rule(slave_id:int, float_list:List) -> tuple[str,str]:
    if not float_list:
        return "",""
    alarm_info = []
    alarm_level = ""
    now_ts = time.time()
    collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    temp = float_list[0] if len(float_list)>=1 else None
    press = float_list[1] if len(float_list)>=2 else None
    with GLOBAL_LOCK:
        alarm_interval = RUNTIME_CONFIG["alarm_interval"]
    if temp is not None and temp>50:
        key = f"{slave_id}_temp"
        if key not in alarm_time_map or now_ts - alarm_time_map[key] > alarm_interval:
            alarm_info.append("温度过高")
            alarm_time_map[key] = now_ts
    if press is not None and press < 0:
        key = f"{slave_id}_press"
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
        with GLOBAL_LOCK:
            alarm_trigger_cnt +=1
    return alarm_str, alarm_level

# ===================== Modbus重连 =====================
def modbus_reconnect(client:ModbusTcpClient) -> bool:
    client.close()
    time.sleep(1)
    ok = client.connect()
    if ok:
        logger.info("Modbus重连成功")
    else:
        logger.error("Modbus重连失败")
    return ok

def read_modbus_regs(client:ModbusTcpClient, slave_id:int, modbus_addr, modbus_count):
    collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    reg_list = []
    float_list = []
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

# ===================== Mysql连接池 =====================
class MysqlPool:
    def __init__(self, cfg:Dict, max_idle:int):
        self.cfg = cfg
        self.max_idle = max_idle
        self.queue:Queue[pymysql.connections.Connection] = Queue(maxsize=self.max_idle)
        for _ in range(max_idle):
            try:
                conn = pymysql.connect(**cfg)
                self.queue.put(conn)
            except Exception:
                logger.error("初始化连接池创建连接失败")
    def get_conn(self) -> Optional[pymysql.connections.Connection]:
        try:
            if not self.queue.empty():
                return self.queue.get()
            return pymysql.connect(**self.cfg)
        except Exception:
            logger.error(f"获取连接失败\n{traceback.format_exc()}")
            return None
    def release_conn(self, conn:Optional[pymysql.connections.Connection]):
        if conn is None:
            return
        try:
            if self.queue.qsize() < self.max_idle:
                self.queue.put(conn)
            else:
                conn.close()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    def close_all(self):
        while not self.queue.empty():
            c = self.queue.get()
            try:
                c.close()
            except Exception:
                pass

# ===================== SQL写入 =====================
def write_to_sql(pool:MysqlPool, batch_list:List[tuple]) -> bool:
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

# ===================== JSON离线缓存 =====================
def save_to_json(batch_list:list):
    with GLOBAL_LOCK:
        cache_file = RUNTIME_CONFIG["cache_file"]
    cache_list = []
    try:
        if os.path.exists(cache_file):
            with open(cache_file,"r",encoding="utf-8") as f:
                cache_list = json.load(f)
    except json.JSONDecodeError:
        logger.warning("缓存文件损坏")
    except Exception:
        logger.error(f"读缓存失败\n{traceback.format_exc()}")
    cache_list.extend(batch_list)
    try:
        with open(cache_file,"w",encoding="utf-8") as f:
            json.dump(cache_list, f, ensure_ascii=False)
    except Exception:
        logger.error(f"写缓存失败\n{traceback.format_exc()}")

def load_and_replay_to_sql(pool:MysqlPool):
    with GLOBAL_LOCK:
        cache_file = RUNTIME_CONFIG["cache_file"]
    if not os.path.exists(cache_file):
        return
    try:
        with open(cache_file,"r",encoding="utf-8") as f:
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
            os.remove(cache_file)
        else:
            logger.error("缓存回放入库失败，保留缓存文件等待重试")
    return

# ===================== 采集主循环 =====================
def collect_main_loop(pool: MysqlPool):
    cfg = RUNTIME_CONFIG
    modbus_cfg = cfg["modbus"]
    slave_dict = cfg["slave_dict"]
    client = ModbusTcpClient(host=modbus_cfg["host"], port=modbus_cfg["port"], timeout=modbus_cfg["timeout"])
    batch = []
    BATCH_SIZE = 100
    while not SHUTDOWN_FLAG:
        for sid,addr in slave_dict.items():
            if SHUTDOWN_FLAG:
                break
            collect_time, reg_list, float_list, status = read_modbus_regs(client, sid, addr, modbus_cfg["count"])
            collect_total +=1
            offline = slave_online_monitor(sid, success=(status=="success"))
            stable = 0
            alarm_str, alarm_level = "", ""
            temp_val = float_list[0] if len(float_list)>=1 else None
            press_val = float_list[1] if len(float_list)>=2 else None
            if status == "success":
                collect_success +=1
                if filter_dirty_regs(reg_list):
                    logger.debug(f"slave{sid}脏寄存器，跳过")
                    continue
                write_to_normal_txt(collect_time, sid, reg_list, float_list, status)
                alarm_str, alarm_level = alarm_rule(sid, float_list)
                # 滑动窗口
                if sid not in stat_data_window:
                    stat_data_window[sid] = []
                stat_data_window[sid].append([temp_val, press_val])
                stable = stat_stable(stat_data_window[sid])
                batch.append((collect_time, sid, str(reg_list), str(float_list), temp_val, press_val, alarm_str, alarm_level, offline, status, stable))
                if len(batch) >= BATCH_SIZE:
                    ok = write_to_sql(pool, batch)
                    if not ok:
                        save_to_json(batch)
                    batch.clear()
            else:
                collect_fail +=1
        # 批次剩余数据入库
        if len(batch) >0:
            ok = write_to_sql(pool, batch)
            if not ok:
                save_to_json(batch)
            batch.clear()
        time.sleep(1)
    # 停机前剩余数据落库
    if len(batch)>0:
        write_to_sql(pool,batch)
    pool.close_all()
    client.close()
    logger.info("采集循环退出")

# ===================== 加载yaml配置 =====================
def load_yaml_config(yaml_path: str = "app.yaml") -> Dict[str, Any]:
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"配置文件不存在:{yaml_path}")
    with open(yaml_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    logger.info("🟢已加载 app.yaml")
    return cfg

# ===================== 程序入口 =====================
def main():
    global RUNTIME_CONFIG
    try:
        RUNTIME_CONFIG = load_yaml_config("app.yaml")
    except Exception as e:
        logger.error(f"加载配置失败 {e}")
        sys.exit(1)
    logger.info("===== 🟢A6 开始程序启动自检 =====")
    # 初始化mysql连接池
    mysql_cfg = RUNTIME_CONFIG["mysql"]
    pool = MysqlPool(mysql_cfg, max_idle=4)
    # 启动离线缓存回放
    load_and_replay_to_sql(pool)
    logger.info("===== 🟢A6 全部自检通过，启动业务线程 =====")
    t_collect = threading.Thread(target=collect_main_loop, args=(pool,), daemon=True)
    t_collect.start()
    while not SHUTDOWN_FLAG:
        time.sleep(0.5)
    logger.info("程序收到停机标记，准备优雅退出")

if __name__ == "__main__":
    main()
