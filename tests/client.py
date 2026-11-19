import socket
import struct

def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("服务器关闭了连接")
        data += chunk
    return data


s = socket.create_connection(("127.0.0.1", 9999))
print("连接服务器成功！输入 exit 退出")

try:
    while True:
        message = input("客户端> ")

        if message == "exit":
            break

        # 1. 字符串转成字节
        body = message.encode("utf-8")

        # 2. 生成4字节长度包头（网络字节序）
        header = struct.pack("!I", len(body))

        # 3. 发送完整数据包
        s.sendall(header + body)

        # 4. 接收服务器返回的长度包头
        resp_header = recv_exact(s, 4)
        body_len = struct.unpack("!I", resp_header)[0]

        # 5. 按长度接收消息体
        response = recv_exact(s, body_len)

        print("服务器>", response.decode("utf-8", errors="replace"))

except (ConnectionError, OSError) as e:
    print("连接异常：", e)
finally:
    s.close()