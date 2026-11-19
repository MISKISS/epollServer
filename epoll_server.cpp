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
    ev.data.u64=0;
    ev.events = EPOLLIN;
    if(epoll_ctl(m_epfd, EPOLL_CTL_ADD, m_lfd, &ev))
    {
        perror("epoll_ctl");
        return false;
    }
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
                uint64_t id=event[i].data.u64;
                uint32_t flags = event[i].events;
                if(id==0)
                {
                    if (!acceptClient()) 
                    {
                        std::cout << "acceptClient is error!" << std::endl;
                    }
                }
                else
                {
                    std::shared_ptr<connection> conn;
                    {
                        std::lock_guard<std::mutex> lock(m_connectionsMutex);
                        auto it= m_connections.find(id);
                        if(it!=m_connections.end())
                        {
                            conn=it->second;
                        }
                        if(!conn||conn->closing)
                        {
                            continue;
                        }
                    }
                    m_pool.addTask([this, conn,flags]()
                    { 
                        std::lock_guard<std::mutex> lock(conn->ioMutex);//防止一个连接同时出现收发删工作
                        if (conn->closing.load())
                        {
                            return;//该连接正在被关闭中
                        }
                        this->handClient(conn,flags);
                    });
                }
            }
        }
        if(num==-1)
        {
            if(errno==EINTR)
            {
                continue;
            }
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
    auto conn =std::make_shared<connection>();
    conn->fd=cfd;
    conn->id=m_nextConnectionId.fetch_add(1);//建立connection
    {
        std::lock_guard<std::mutex> lock(m_connectionsMutex);
        m_connections[conn->id]=conn;
    }
    epoll_event nev;
    nev.data.u64=conn->id;
    nev.events = EPOLLIN | EPOLLONESHOT;
    if (epoll_ctl(m_epfd, EPOLL_CTL_ADD, cfd, &nev) == -1)
    {
       perror("epoll_ctl ADD");
        close(cfd);
        return false;
    }
    
    std::cout << "客户端连接成功，id= " << conn->id << std::endl;
    return true;
}
bool epollServer::handClient(std::shared_ptr<connection> conn,uint32_t flags)
{
    int fd = conn->fd;
    if (flags & (EPOLLERR | EPOLLHUP))
    {
        closeClient(conn);
        return false;
    }
    if (flags & EPOLLIN)
    {
        if(!handleRead(conn))
        {
            return false;
        }
    }
    if(!handleWrite(conn))
    {
        return false;
    }

    return true;
}
bool epollServer::handleRead(std::shared_ptr<connection> conn)
{
    int fd = conn->fd;
    char buf[1024];
    while (1)
    {
        ssize_t lens = recv(fd, buf, 1024, 0);
        if (lens == 0)
        {
            std::cout << "客户端[" << conn->id << "]断开了连接！" << std::endl;
            closeClient(conn);
            return false;
        }
        else if (lens > 0)
        {
            std::string message;
            conn->m_recvBuffer.append(buf, lens);
            while (1)
            {
                Message::DecodeResult result;
                result = Message::decode(conn->m_recvBuffer, message);
                if (result == Message::DecodeResult::Complete)
                {
                    std::cout << "客户端[ " << conn->id << " ]" << "说: " << std::endl;
                    std::string packet = Message::encode(message);
                    std::cout.write(message.data(), message.size());
                    std::cout << std::endl;
                    conn->m_pending.append(packet);
                    continue;
                }
                else if (result == Message::DecodeResult::Incomplete)
                {
                    break;
                }
                else if (result == Message::DecodeResult::Invalid)
                {
                    std::cout << "客户端[" << conn->id << "]" << "发送了非法数据，已关闭" << std::endl;

                    closeClient(conn);
                    return false;
                }
            }
            continue;
        }
        else if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            break;
        }
        else if (errno == EINTR)
        {
            continue;
        }
        else
        {
            perror("recv");
            closeClient(conn);
            return false;
        }
        }
    return true;
}
bool epollServer::handleWrite(std::shared_ptr<connection> conn)
{
    int fd = conn->fd;
    bool sendError = false;
            std::string& data = conn->m_pending;
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
    
    if (sendError)
    {
        perror("send");
        closeClient(conn);
        return false;
    }
    if(!rearmClient(conn))
    {
        return false;
    }

    return true;
}
bool epollServer::rearmClient(std::shared_ptr<connection> conn)
{
    int fd = conn->fd;

    epoll_event ev{};
    ev.data.u64 = conn->id;
    ev.events = EPOLLIN | EPOLLONESHOT;
    if(!conn->m_pending.empty())
    {
        ev.events |= EPOLLOUT;
    }
    if (epoll_ctl(m_epfd, EPOLL_CTL_MOD, fd, &ev) == -1)
    {
        perror("epoll_ctl");
        closeClient(conn);
        return false;
    }

    
    return true;
}
bool epollServer::setNonBlocking(int fd)//变成LT模式
{
    int flags = fcntl(fd, F_GETFL, 0);

    if (flags == -1)
        return false;

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
        return false;

    return true;
}
void epollServer::closeClient(std::shared_ptr<connection> conn)
{
    int fd = conn->fd;
    if(conn->closing.exchange(true))
    {
        return;
    }
    conn->m_pending.clear();
    conn->m_recvBuffer.clear();
    {
        std::lock_guard<std::mutex> lock(m_connectionsMutex);
        m_connections.erase(conn->id);
    }
    epoll_ctl(m_epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    conn->fd = -1;
    return;
}