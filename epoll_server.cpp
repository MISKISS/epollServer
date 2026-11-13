#include"epoll_server.h"
#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>
#include <cerrno>
epollServer::epollServer(int port):m_port(port),m_pool(3,10),m_lfd(-1),m_epfd(-1)
{

}
epollServer::~epollServer()
{

}
bool epollServer::start()
{
    if(!init())
    {
        return false;
    }
    eventLoop();
    return true;
}
bool epollServer::init()
{
    m_lfd=socket(AF_INET,SOCK_STREAM,0);
    if(m_lfd==-1)
    {
        perror("m_lfd");
        return false;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(m_port);
    addr.sin_addr.s_addr = INADDR_ANY;
    if(bind(m_lfd,(sockaddr*)&addr,sizeof(addr))==-1)
    {
        perror("bind");
        return false;
    }
    if(listen(m_lfd,128))
    {
        perror("listen");
        return false;
    }
    m_epfd = epoll_create1(0);
    if(m_epfd==-1)
    {
        perror("epoll_create");
        return false;
    }
    epoll_event ev;
    ev.data.fd = m_lfd;
    ev.events = EPOLLIN;
    epoll_ctl(m_epfd, EPOLL_CTL_ADD, m_lfd, &ev);
    return true;
}
void epollServer::eventLoop()
{
    epoll_event event[1024];
    while(1)
    {
        int num=epoll_wait(m_epfd, event, 1024, -1);
        if(num>0)
        {   
            for (int i = 0; i < num;++i)
            {
                int nfd = event[i].data.fd;
                uint32_t flags = event[i].events;
                if(nfd==m_lfd)
                {
                    if (!acceptClient()) 
                    {
                        std::cout << "acceptClient is error!" << std::endl;
                    }
                }
                else
                {
                    m_pool.addTask([this, nfd,flags]()
                    { 
                    this->handClient(nfd,flags); 
                    });
                }
            }
        }
        if(num==-1)
        {
            perror("epoll_wait");
            return;
        }
    }
}

bool epollServer::acceptClient()
{
    sockaddr caddr{};
    socklen_t caddrl=sizeof(caddr);
    int cfd=accept(m_lfd, &caddr, &caddrl);
    if(cfd==-1)
    {
        perror("accpet");
        return false;
    }
    if(!setNonBlocking(cfd))
    {
        perror("setNonBlocking");
        close(cfd);
        return false;
    }
    epoll_event nev;
    nev.data.fd = cfd;
    nev.events = EPOLLIN | EPOLLONESHOT;
    if (epoll_ctl(m_epfd, EPOLL_CTL_ADD, cfd, &nev) == -1)
    {
       perror("epoll_ctl ADD");
        close(cfd);
        return false;
    }
    std::cout << "客户端连接成功，cfd= " << cfd << std::endl;
    return true;
}
bool epollServer::handClient(int fd,uint32_t flags)
{
    if (flags & (EPOLLERR | EPOLLHUP))
    {
        closeClient(fd);
        return false;
    }
    if (flags & EPOLLIN)
    {
        char buf[1024];
        std::cout << "客户端[ " << fd << " ]" << "说: " << std::endl;
        while (1)
        {
            ssize_t lens = recv(fd, buf, 1024, 0);
            if (lens == 0)
            {
                std::cout << "客户端["<<fd<<"]断开了连接！" << std::endl;
                closeClient(fd);
                return false;
            }
            else if(lens>0)
            {
                std::cout.write(buf, lens);
                std::lock_guard<std::mutex> lock(m_pendingMutex);
                m_pending[fd].append(buf, lens);
                continue;
            }
            else if (errno==EAGAIN||errno==EWOULDBLOCK)
            {
                break;
            }
            else if(errno == EINTR)
            {
                continue;
            }
            else
            {
                perror("recv");
                closeClient(fd);
                return false;
            }
        }
        
    }
    bool needWirte = false;
    bool sendError = false;
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);

        auto it = m_pending.find(fd);
        if(it!=m_pending.end()&&!it->second.empty())
        {
            std::string &data = it->second;
            while(!data.empty())
            {
                ssize_t sent = send(fd, data.data(), data.size(), MSG_NOSIGNAL);
                if (sent > 0)
                {
                    // 删除已经成功提交的前 sent 字节
                    data.erase(0, sent);
                    continue;
                }
                else if(sent<0)
                {
                    if (errno == EINTR)
                    {
                        continue;
                    }

                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        break;
                    }
                    sendError = true;
                    break;
                }
                if(sent==0)
                {
                    break;
                }
            }
            if(data.empty())
            {
                m_pending.erase(it);
            }
            else
            {
                needWirte = true;
            }
        }
    }
    if(sendError)
    {
        perror("send");
        closeClient(fd);
        return false;
    }
    epoll_event ev{};
    ev.data.fd = fd;
    ev.events = EPOLLIN | EPOLLONESHOT;
    if(needWirte)
    {
        ev.events |= EPOLLOUT;
    }
    if (epoll_ctl(m_epfd, EPOLL_CTL_MOD, fd, &ev) == -1)
    {
        perror("epoll_ctl MOD");
        closeClient(fd);
       return false;
    }
    return true;
}
bool epollServer::setNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);

    if (flags == -1)
        return false;

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
        return false;

    return true;
}
void epollServer::closeClient(int fd)
{
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pending.erase(fd);
    }
    epoll_ctl(m_epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
}