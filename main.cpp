#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include"epoll_server.h"
int main()
{
    epollServer epollserver(9999);
    if(!epollserver.start())
    {
        std::cout << "服务器启动失败" << std::endl;
        return 0;
    }
    return true;
}