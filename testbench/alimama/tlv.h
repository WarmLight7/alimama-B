#include <iostream>
#include <string>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <vector>

// RPC请求与响应编码
struct rpcRequest
{
    std::vector<uint64_t> keywords;
    float context_vector[2];
    uint64_t hour;
    uint64_t topn;
    uint64_t requestID;

    void print()
    {
        std::cout << "  keywords: ";
        for(auto& keyword:keywords)
        {
            std::cout<<keyword<<" ";
        }
        std::cout << std::endl;
        std::cout << "  context_vector: " << context_vector[0] << " " << context_vector[1] << std::endl;
        std::cout << "  hour: " << hour << std::endl;
        std::cout << "  topn: " << topn << std::endl;
        std::cout << "  requestID: " << requestID << std::endl;
        std::cout<<std::endl;
    }
};
struct rpcResponse
{
    std::vector<uint64_t> adgroup_ids;
    std::vector<uint64_t> prices;
    uint64_t responseID;

    void print()
    {
        std::cout << "  adgroup_ids: ";
        for(auto& adgroup_id:adgroup_ids)
        {
            std::cout<<adgroup_id<<" ";
        }
        std::cout<<std::endl;
        std::cout << "  prices: ";
        for(auto& price:prices)
        {
            std::cout<<price<<" ";
        }
        std::cout<<std::endl;
        std::cout << "  responseID: " << responseID << std::endl;
    }
};
void RPCRequest2Bytes(rpcRequest& req, char* buffer)
{
    buffer[0] = 0; // TLV 类型 0：RPC请求 1：RPC响应
    //编码长度

    int index = 3; // 1,2字节存储长度
    std::memcpy(buffer + index, reinterpret_cast<char*>(&req.hour), sizeof(uint64_t));

    index += sizeof(uint64_t);
    std::memcpy(buffer + index, reinterpret_cast<char*>(&req.topn), sizeof(uint64_t));

    index += sizeof(uint64_t);
    std::memcpy(buffer + index, reinterpret_cast<char*>(&req.requestID), sizeof(uint64_t));

    index += sizeof(uint64_t);
    std::memcpy(buffer + index, reinterpret_cast<char*>(&req.context_vector[0]), sizeof(float));

    index += sizeof(float);
    std::memcpy(buffer + index, reinterpret_cast<char*>(&req.context_vector[1]), sizeof(float));

    index += sizeof(float);
    for(int j = 0; j < req.keywords.size(); j++) 
    {
        std::memcpy(buffer + index, reinterpret_cast<char*>(&req.keywords[j]), sizeof(uint64_t));
        index += sizeof(uint64_t);
    }

    // TLV 存储长度
    buffer[1] = index >> 8;  // 长度高字节
    buffer[2] = index & 0xFF; // 长度低字节
}
void Bytes2RPCRequest(rpcRequest& req, const char* buffer) 
{
    // type = buffer[0];
    int length = (buffer[1] << 8) | buffer[2];

    int index = 3;
    std::memcpy(&req.hour, &buffer[index], sizeof(uint64_t));

    index += sizeof(uint64_t);
    std::memcpy(&req.topn, &buffer[index], sizeof(uint64_t));

    index += sizeof(uint64_t);
    std::memcpy(&req.requestID, &buffer[index], sizeof(uint64_t));

    index += sizeof(uint64_t); // 在这个地方开始copy
    for(int j = 0; j < 2; j++) 
    {
        std::memcpy(&req.context_vector[j], &buffer[index], sizeof(float));
        index += sizeof(float);
    }
    // 解析keyword数组
    for(; index < length; index+=sizeof(uint64_t)) 
    {
        uint64_t temp = 0;
        std::memcpy(&temp, &buffer[index], sizeof(uint64_t));
        req.keywords.push_back(temp);
    }
}
void RPCResponse2Bytes(rpcResponse& res, char* buffer)
{
    buffer[0] = 1; // // TLV 类型 0：RPC请求 1：RPC响应
    //编码长度

    int index = 3; // 1,2字节存储长度

    std::memcpy(buffer + index, reinterpret_cast<char*>(&res.responseID), sizeof(uint64_t));
    index += sizeof(uint64_t);

    for(int j = 0; j < res.adgroup_ids.size(); j++) 
    {
        std::memcpy(buffer + index, reinterpret_cast<char*>(&res.adgroup_ids[j]), sizeof(uint64_t));
        index += sizeof(uint64_t);
    }
    for(int j = 0; j < res.prices.size(); j++) 
    {
        std::memcpy(buffer + index, reinterpret_cast<char*>(&res.prices[j]), sizeof(uint64_t));
        index += sizeof(uint64_t);
    }  

    // TLV 存储长度
    buffer[1] = index >> 8;  // 长度高字节
    buffer[2] = index & 0xFF; // 长度低字节
}
void Bytes2RPCResponse(rpcResponse& res, const char* buffer) 
{
    // type = buffer[0];
    int length = (buffer[1] << 8) | buffer[2];

    int index = 3;

    std::memcpy(&res.responseID, &buffer[index], sizeof(uint64_t));
    index += sizeof(uint64_t);

    // 解析adgroup_ids数组
    for(; index < 3 + 8 + (length - 3 - 8) / 2; index += sizeof(uint64_t)) 
    {
        uint64_t temp = 0;
        std::memcpy(&temp, &buffer[index], sizeof(uint64_t));
        res.adgroup_ids.push_back(temp);
    }

    // 解析prices数组
    for(; index < length; index+=sizeof(uint64_t)) 
    {
        uint64_t temp = 0;
        std::memcpy(&temp, &buffer[index], sizeof(uint64_t));
        res.prices.push_back(temp);
    }    
}

class TLVServer {
private:
    int sockfd;
    struct sockaddr_in serverAddr, clientAddr;
    socklen_t addrLen;
    char buffer[1024]; // UDP数据帧最大载荷

    int _port;
public:
    TLVServer(int port = 12345)
    {
        _port = port;

        // 创建UDP套接字
        addrLen = sizeof(clientAddr);
        sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd < 0) {
            std::cerr << "Failed to create socket." << std::endl;
            return;
        }

        // 设置服务器地址和端口
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(_port);
        serverAddr.sin_addr.s_addr = INADDR_ANY;

        // 将套接字绑定到服务器地址
        if (bind(sockfd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
            std::cerr << "Failed to bind socket." << std::endl;
            return;
        }
    }
    int TLVListening()
    {
        while (true) {
            // 接收数据
            memset(buffer, 0, sizeof(buffer));
            ssize_t numBytes = recvfrom(sockfd, buffer, sizeof(buffer), 0, (struct sockaddr*)&clientAddr, &addrLen);
            if (numBytes < 0) {
                std::cerr << "Failed to receive data." << std::endl;
                return 1;
            }
            // 解析字节流的TLV数据
            if( (int)buffer[0] == 1) //rpc响应
            {
                rpcResponse res;
                Bytes2RPCResponse(res,buffer);
                // 打印收到的TLV内容
                std::cout << "Received TLV:" << std::endl;
                std::cout << "Type: " << (int)buffer[0] << std::endl;
                std::cout << "Length: " << (int)((buffer[1] << 8) | buffer[2]) << std::endl;
                std::cout << "Value: " << std::endl;
                res.print();
            }
            else //rpc请求
            {
                rpcRequest req;
                Bytes2RPCRequest(req, buffer);
                // 打印收到的TLV内容
                std::cout << "Received TLV:" << std::endl;
                std::cout << "Type: " << (int)buffer[0] << std::endl;
                std::cout << "Length: " << (int)((buffer[1] << 8) | buffer[2]) << std::endl;
                std::cout << "Value: " << std::endl;
                req.print();
            }
        }
    }
    ~TLVServer()
    {
        close(sockfd);
    }
};

class TLVClient {
private:
    int sockfd;
    struct addrinfo hints, *res;

    std::string _hostname;
    int _port;
public:
    TLVClient(const std::string& hostname, int port = 12345)
    {
        _hostname = hostname;
        _port = port;

        // 设置hints结构体
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_INET;      // IPv4
        hints.ai_socktype = SOCK_DGRAM; // UDP协议

        // 解析主机名
        int status = getaddrinfo(_hostname.c_str(), std::to_string(port).c_str(), &hints, &res);
        if (status != 0) {
            std::cerr << "getaddrinfo error: " << gai_strerror(status) << std::endl;
            return;
        }

        // 创建套接字
        sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (sockfd == -1) {
            std::cerr << "socket error" << std::endl;
            freeaddrinfo(res);
            return;
        }        
    }
    void TLVSendMessage(char* buffer, int length)
    {
        // 发送消息
        ssize_t numBytesSent = sendto(sockfd, buffer, length, 0, res->ai_addr, res->ai_addrlen);
        if (numBytesSent == -1) {
            std::cerr << "sendto error" << std::endl;
            close(sockfd);
            freeaddrinfo(res);
            return;
        }
        std::cout << "Sent " << numBytesSent << " bytes to " << _hostname << ":" << _port << std::endl;
    }
    ~TLVClient()
    {
        close(sockfd);
        freeaddrinfo(res);
    }
};