import socket
import struct
import os
import time
import subprocess

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

# ============ 测试配置 ============

HOST = "127.0.0.1"
PORT = 9999

WORKERS = 100
ROUNDS = 200
TEST_ROUNDS = 15

CONNECT_TIMEOUT = 4
IO_TIMEOUT = 5

# =================================


def packet(body):
    """生成 Header + Body 数据包"""
    return struct.pack("!I", len(body)) + body


def recv_exact(sock, n):
    """准确接收 n 个字节"""
    data = bytearray()

    while len(data) < n:
        chunk = sock.recv(n - len(data))

        if not chunk:
            raise ConnectionError("服务器提前关闭连接")

        data.extend(chunk)

    return bytes(data)


def check_echo(sock, expected):
    """检查回显数据是否与发送数据完全一致"""

    header = recv_exact(sock, 4)
    length = struct.unpack("!I", header)[0]

    if length > 65536:
        raise ValueError(f"非法回显长度：{length}")

    actual = recv_exact(sock, length)

    if actual != expected:
        raise AssertionError(
            f"回显数据不一致，预期长度={len(expected)}，"
            f"实际长度={len(actual)}"
        )


def get_server_pid():
    """自动查找 C++ 服务器进程"""

    try:
        output = subprocess.check_output(
            ["pgrep", "-n", "-x", "main"],
            text=True
        )

        return int(output.strip())

    except (subprocess.CalledProcessError, FileNotFoundError):
        print("错误：找不到 main 服务器进程")
        raise SystemExit(1)


def get_resources(pid):
    """读取 Linux 服务器进程资源"""

    fd_path = f"/proc/{pid}/fd"
    status_path = f"/proc/{pid}/status"

    fd_count = len(os.listdir(fd_path))

    rss_kb = 0

    with open(status_path, "r") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                rss_kb = int(line.split()[1])
                break

    return fd_count, rss_kb


def worker(worker_id):
    """单个客户端线程"""

    results = []

    for round_id in range(ROUNDS):

        mode = round_id % 7

        unique = os.urandom(8).hex()

        msg = (
            f"client={worker_id},"
            f"round={round_id},"
            f"id={unique}"
        ).encode()

        stage = "建立连接"

        try:
            with socket.create_connection(
                (HOST, PORT),
                timeout=CONNECT_TIMEOUT
            ) as s:

                s.settimeout(IO_TIMEOUT)

                if mode == 0:
                    # 普通消息
                    stage = "普通回显"

                    s.sendall(packet(msg))
                    check_echo(s, msg)

                elif mode == 1:
                    # 粘包：连续发送两个数据包
                    stage = "粘包测试"

                    msg2 = msg + b"-second"

                    s.sendall(
                        packet(msg) + packet(msg2)
                    )

                    check_echo(s, msg)
                    check_echo(s, msg2)

                elif mode == 2:
                    # 拆包：分多次发送
                    stage = "拆包测试"

                    data = packet(msg)

                    s.sendall(data[:2])
                    time.sleep(0.005)

                    s.sendall(data[2:5])
                    time.sleep(0.005)

                    s.sendall(data[5:])

                    check_echo(s, msg)

                elif mode == 3:
                    # 大数据回显
                    stage = "32KB 数据测试"

                    big = msg + os.urandom(32768)

                    s.sendall(packet(big))
                    check_echo(s, big)

                elif mode == 4:
                    # 只发送部分数据后断开
                    stage = "不完整数据包断连"

                    s.sendall(packet(msg)[:6])

                elif mode == 5:
                    # 完整发送后立即断开
                    stage = "发送完成后立即断连"

                    s.sendall(packet(msg))

                elif mode == 6:
                    # 非法长度
                    stage = "非法长度测试"

                    s.sendall(
                        struct.pack("!I", 70000)
                    )

            if mode < 4:
                results.append(("normal", None))
            else:
                results.append(("injection", None))

        except Exception as e:

            error = (
                f"线程={worker_id}, "
                f"轮次={round_id}, "
                f"模式={mode}, "
                f"阶段={stage}, "
                f"错误={type(e).__name__}: {e}"
            )

            results.append(("failed", error))

    return results


def run_stress_test():
    """运行一轮压力测试"""

    print(f"目标服务器：{HOST}:{PORT}")
    print(f"并发线程：{WORKERS}")
    print(f"本轮连接次数：{WORKERS * ROUNDS}")
    print("开始压力测试...\n")

    start = time.perf_counter()

    all_results = []

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:

        futures = [
            pool.submit(worker, i)
            for i in range(WORKERS)
        ]

        for future in as_completed(futures):
            all_results.extend(future.result())

    duration = time.perf_counter() - start

    counts = Counter(
        kind for kind, _ in all_results
    )

    errors = [
        detail
        for kind, detail in all_results
        if kind == "failed"
    ]

    print("\n" + "=" * 50)
    print("压力测试结果")
    print("=" * 50)

    print(f"总连接次数：     {len(all_results)}")
    print(f"正常回显通过：   {counts['normal']}")
    print(f"异常流量注入：   {counts['injection']}")
    print(f"客户端失败：     {counts['failed']}")
    print(f"耗时：           {duration:.2f} 秒")

    if errors:
        print("\n前 10 个错误：")

        for error in errors[:10]:
            print(error)

        print("\nFAIL：存在客户端测试失败")

    else:
        print("\nPASS：正常回显全部通过")
        print("异常流量已发送，服务端清理需结合资源检查")

    return {
        "duration": duration,
        "normal": counts["normal"],
        "injection": counts["injection"],
        "failed": counts["failed"]
    }


def run_all_tests():
    """自动运行多轮并监控资源"""

    pid = get_server_pid()

    print("=" * 55)
    print("epollServer 自动化压力与资源测试")
    print("=" * 55)

    print(f"服务器 PID：{pid}")
    print(f"并发线程数：{WORKERS}")
    print(f"每轮连接数：{WORKERS * ROUNDS}")
    print(f"测试轮数：{TEST_ROUNDS}")
    print(f"预计总连接数：{WORKERS * ROUNDS * TEST_ROUNDS}")

    initial_fd, initial_rss = get_resources(pid)

    print("\n初始资源：")
    print(f"fd 数量：{initial_fd}")
    print(f"RSS 内存：{initial_rss} KB")

    reports = []

    for i in range(1, TEST_ROUNDS + 1):

        print("\n" + "#" * 55)
        print(f"第 {i} / {TEST_ROUNDS} 轮测试")
        print("#" * 55)

        report = run_stress_test()

        # 等待服务端完成连接清理
        time.sleep(1)

        try:
            fd, rss = get_resources(pid)

        except (FileNotFoundError, ProcessLookupError):
            print("FAIL：服务器进程已经退出！")
            raise SystemExit(1)

        report["fd"] = fd
        report["rss"] = rss

        reports.append(report)

        print("\n本轮服务器资源：")
        print(f"fd 数量：{fd}")
        print(f"fd 变化：{fd - initial_fd:+d}")

        print(f"RSS 内存：{rss} KB")
        print(f"RSS 变化：{rss - initial_rss:+d} KB")

    # ============ 最终报告 ============

    total_connections = WORKERS * ROUNDS * TEST_ROUNDS

    total_normal = sum(
        r["normal"] for r in reports
    )

    total_injection = sum(
        r["injection"] for r in reports
    )

    total_failed = sum(
        r["failed"] for r in reports
    )

    total_time = sum(
        r["duration"] for r in reports
    )

    avg_time = total_time / TEST_ROUNDS

    final_fd, final_rss = get_resources(pid)

    print("\n\n" + "=" * 60)
    print("              最终自动化测试报告")
    print("=" * 60)

    print(f"服务器 PID：       {pid}")
    print(f"并发线程：         {WORKERS}")
    print(f"测试轮数：         {TEST_ROUNDS}")
    print(f"总连接操作：       {total_connections}")

    print(f"\n正常回显通过：     {total_normal}")
    print(f"异常流量注入：     {total_injection}")
    print(f"客户端失败：       {total_failed}")

    print(f"\n总测试耗时：       {total_time:.2f} 秒")
    print(f"平均每轮耗时：     {avg_time:.2f} 秒")

    print("\n各轮详细结果：")

    print(
        f"{'轮次':<8}"
        f"{'耗时(s)':<12}"
        f"{'失败':<10}"
        f"{'fd':<10}"
        f"{'RSS(KB)':<12}"
    )

    for i, r in enumerate(reports, 1):
        print(
            f"{i:<8}"
            f"{r['duration']:<12.2f}"
            f"{r['failed']:<10}"
            f"{r['fd']:<10}"
            f"{r['rss']:<12}"
        )

    print("\n资源变化：")

    print(
        f"fd：{initial_fd} -> {final_fd} "
        f"({final_fd - initial_fd:+d})"
    )

    print(
        f"RSS：{initial_rss} -> {final_rss} KB "
        f"({final_rss - initial_rss:+d} KB)"
    )

    print("\n" + "=" * 60)

    if total_failed == 0:
        print("PASS：所有需要回显的客户端测试均通过")
    else:
        print(f"FAIL：共有 {total_failed} 次客户端操作失败")

    if final_fd == initial_fd:
        print("PASS：fd 数量恢复初始水平")
    else:
        print("WARNING：fd 数量未恢复初始水平")

    print("\n说明：")
    print("1. 异常流量注入成功不代表服务器清理逻辑完全正确。")
    print("2. fd 数量稳定不能单独证明完全没有资源泄漏。")
    print("3. RSS 增长也不一定意味着内存泄漏。")
    print("4. 本测试为本机短连接压力测试，不代表最大并发连接数。")

    print("=" * 60)

    if total_failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    run_all_tests()