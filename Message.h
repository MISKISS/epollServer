#pragma
#include<string>
#include<cstdint>
#include <arpa/inet.h>
class Message
{
public:
    enum class DecodeResult
    {
        Complete,    // 成功解析一条消息
        Incomplete,  // 数据不完整
        Invalid      // 非法数据包
    };
    static std::string encode(const std::string &message);
    static DecodeResult decode( std::string &buffer, std::string &message);
    static constexpr uint32_t MAX_BODY_SIZE = 64 * 1024;
};