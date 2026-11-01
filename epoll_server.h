#pragma once
class epollServer
{
    public:
        epollServer(int port);
        ~epollServer();
        bool start();

    private:
        bool init();
        void eventLoop();
        bool acceptClient();
        bool handClient(int fd);
        int m_port;
        int m_lfd;
        int m_epfd;
};