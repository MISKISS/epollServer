#include "Message.h"
#include <cstring>

std::string Message::encode(const std::string &message)
{
    uint32_t datalens = message.size();
    uint32_t netlen = htonl(datalens);
    std::string packet;

    packet.append(reinterpret_cast<const char *>(&netlen), sizeof(netlen));
    packet.append(message);
    return packet;
}

Message::DecodeResult Message::decode(std::string &buffer, std::string &message)
{
    if(buffer.size()<sizeof(uint32_t))
    {
        return DecodeResult::Incomplete;
    }
    uint32_t netlen = 0;
    std::memcpy(&netlen, buffer.data(), sizeof(netlen));
    uint32_t datalen = ntohl(netlen);
    if(datalen>MAX_BODY_SIZE)
    {
        return DecodeResult::Invalid;
    }
    size_t totallen = sizeof(datalen) + datalen;
    if(buffer.size()<totallen)
    {
        return DecodeResult::Incomplete;
    }
    message.assign(buffer.data() + sizeof(netlen), datalen);
    buffer.erase(0, totallen);

    return DecodeResult::Complete;
}
