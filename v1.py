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