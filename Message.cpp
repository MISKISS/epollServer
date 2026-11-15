#include "Message.h"
#include <cstring>

std::string Message::encode(const std::string &message)
{
    uint32_t datalens = message.size();//获取信息长度
    uint32_t netlen = htonl(datalens);//把长度转成网络字节序
    std::string packet;//定义字符串类型包

    packet.append(reinterpret_cast<const char *>(&netlen), sizeof(netlen));//把长度数据存入包头
    packet.append(message);//把信息放入包
    return packet;
}

Message::DecodeResult Message::decode(std::string &buffer, std::string &message)
{
    if(buffer.size()<sizeof(uint32_t))
    {
        return DecodeResult::Incomplete;//不完整数据包
    }
    uint32_t netlen = 0;
    std::memcpy(&netlen, buffer.data(), sizeof(netlen));//把包头的数据取出到netlen
    uint32_t datalen = ntohl(netlen);//把网络字节序转换回来
    if(datalen>MAX_BODY_SIZE)//判断是否超出包长度
    {
        return DecodeResult::Invalid;
    }
    size_t totallen = sizeof(datalen) + datalen;//计算包总长度，信息长度 + 包头
    if(buffer.size()<totallen)
    {
        return DecodeResult::Incomplete;//判断包是否有缺失
    }
    message.assign(buffer.data() + sizeof(netlen), datalen);//把地址在buffer包头后的数据全部接进message
    buffer.erase(0, totallen);//把收到的数据删除

    return DecodeResult::Complete;
}
