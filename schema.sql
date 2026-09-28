-- schema.sql
-- Modbus 工控采集网关 - 建表 SQL
-- 执行方式：mysql -u root -p modbus_db < schema.sql

-- 业务数据主表
CREATE TABLE IF NOT EXISTS modbus_day2_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    collect_time DATETIME NOT NULL COMMENT '采集时间',
    slave_id INT NOT NULL COMMENT 'Modbus从站ID',
    reg_list JSON COMMENT '原始寄存器列表',
    float_list JSON COMMENT '解析后的float列表',
    temperature FLOAT COMMENT '温度',
    pressure FLOAT COMMENT '压力',
    alarm_str VARCHAR(255) COMMENT '告警描述',
    alarm_level VARCHAR(50) COMMENT '告警等级',
    offline TINYINT DEFAULT 0 COMMENT '0在线 1离线',
    status VARCHAR(20) COMMENT '采集状态',
    stable TINYINT DEFAULT 0 COMMENT '0不稳定 1稳定',
    UNIQUE KEY uk_slave_time (slave_id, collect_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Modbus采集业务数据表';

-- 历史备份表
CREATE TABLE IF NOT EXISTS modbus_history_backup (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    collect_time DATETIME NOT NULL,
    slave_id INT NOT NULL,
    reg_list JSON,
    float_list JSON,
    UNIQUE KEY uk_slave_time (slave_id, collect_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Modbus采集历史备份表';