#pragma once
#include"ThreadPool.h"
#include <unordered_map>
#include <string>
#include <mutex>
#include <cstdint>
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
        bool handClient(int fd,uint32_t flags);
        bool setNonBlocking(int fd);
        // 每个 fd 对应自己尚未发送的数据
        std::unordered_map<int, std::string> m_pending;
        // 保护 m_pending，避免多个工作线程同时操作 map
        std::mutex m_pendingMutex;
        void closeClient(int fd);
        int m_port;
        int m_lfd;
        int m_epfd;
};