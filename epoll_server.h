#pragma once
#include"ThreadPool.h"
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
        bool handClient(int fd);
        int m_port;
        int m_lfd;
        int m_epfd;
};