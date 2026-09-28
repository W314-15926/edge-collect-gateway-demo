"""
Modbus模拟器写测试值脚本（修正版）
往 Modbus Slave 模拟器写入「合法的 32 位 float」寄存器值，
让你的 main.py 能正确解析出温度和压力，而不是 nan。

对接：Modbus Slave 模拟器，slave_id=1，端口502
"""
import struct
from pymodbus.client import ModbusTcpClient


def float_to_regs(f: float):
    """把 32 位 float 拆成两个 16 位寄存器（大端，匹配 main.py 的 >HH / >f）"""
    b = struct.pack(">f", f)
    r1, r2 = struct.unpack(">HH", b)
    return r1, r2


def main():
    # ----------------配置区----------------
    HOST = "127.0.0.1"
    PORT = 502
    DEVICE_ID = 1
    # --------------------------------------

    client = ModbusTcpClient(host=HOST, port=PORT)
    if not client.connect():
        print("❌ 连接模拟器失败，请确认 Modbus Slave 已启动，端口502，从站ID=1")
        return

    print("✅ Modbus连接成功\n")

    # ========= 1. 写入正常的温度和压力（32位float，大端） =========
    # main.py 读 addr=0, count=4，所以：
    #   寄存器0、1 → 温度
    #   寄存器2、3 → 压力

    temperature = 25.5
    pressure = 0.8

    t1, t2 = float_to_regs(temperature)
    p1, p2 = float_to_regs(pressure)

    print("---【写入正常业务值（32位float大端）】---")
    print(f"温度 {temperature} → 寄存器[0]={t1}, 寄存器[1]={t2}")
    print(f"压力 {pressure} → 寄存器[2]={p1}, 寄存器[3]={p2}")

    resp = client.write_registers(address=0, values=[t1, t2, p1, p2], device_id=DEVICE_ID)
    if resp.isError():
        print(f"❌ 写入失败: {resp}")
    else:
        print("✅ 地址0~3 写入成功")

    # ========= 2. 写入触发告警的值（温度>50 触发一级警报） =========
    # 想演示告警时，把下面这段打开

    # temperature_alarm = 88.8
    # pressure_alarm = -1.5
    # ta1, ta2 = float_to_regs(temperature_alarm)
    # pa1, pa2 = float_to_regs(pressure_alarm)
    # print(f"\n---【写入告警值】温度 {temperature_alarm} 压力 {pressure_alarm}---")
    # resp = client.write_registers(address=0, values=[ta1, ta2, pa1, pa2], device_id=DEVICE_ID)
    # if resp.isError():
    #     print(f"❌ 告警值写入失败: {resp}")
    # else:
    #     print("✅ 告警值写入成功")

    # ========= 3. 写入脏数据（地址4放 65535，演示脏寄存器过滤） =========
    # 注意：main.py 只读 addr=0, count=4，地址4不在读取范围。
    # 想演示脏数据，把 addr 改成 0 count 改成 6，再打开下面这段。

    print("\n---【写入脏数据】地址4 = 65535---")
    resp = client.write_register(address=4, value=65535, device_id=DEVICE_ID)
    if resp.isError():
        print(f"❌ 脏数据写入失败: {resp}")
    else:
        print("✅ 地址4 = 65535")

    client.close()
    print("\n🎉 写入完成！现在启动 main.py，查看采集日志核对读出数值")


if __name__ == "__main__":
    main()