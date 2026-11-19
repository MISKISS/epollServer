#pragma once
#include"ThreadPool.h"
#include"Message.h"
#include <unordered_map>
#include <string>
#include <mutex>
#include <cstdint>
#include<atomic>
#include<cstdint>
#include <memory>

struct connection
{
    int fd;
    uint64_t id;
    std::atomic<bool> closing{false};
    std::mutex ioMutex;
};
class epollServer
{
    public:
        epollServer(int port);
        ~epollServer();
        bool start();

    private:
        ThreadPool m_pool;
        bool init();
        void eventLoop();
        bool acceptClient();
        bool handClient(std::shared_ptr<connection> conn,uint32_t flags);
        bool handleRead(std::shared_ptr<connection> conn);
        bool handleWrite(std::shared_ptr<connection> conn);
        bool rearmClient(std::shared_ptr<connection> conn);
        bool setNonBlocking(int fd);
        // 每个 fd 对应自己尚未发送的数据
        std::unordered_map<int, std::string> m_pending;//发送缓冲区
        std::unordered_map<int, std::string> m_recvBuffer;//接受缓冲区
        std::unordered_map<int, std::shared_ptr<connection>> m_connections;//存储连接的client
        // 保护 m_pending，避免多个工作线程同时操作 map
        std::mutex m_pendingMutex;
        std::mutex m_recvBufferMutex;
        std::mutex m_connectionsMutex;//防止多个线程同时修改连接表
        std::atomic<uint64_t> m_nextConnectionId{1};
        void closeClient(std::shared_ptr<connection> conn);
        int m_port;
        int m_lfd;
        int m_epfd;
};