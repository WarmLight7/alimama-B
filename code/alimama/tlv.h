#include <iostream>
#include <cstd::string>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

// TLV结构体
class TLV {
private:
    uint8_t type;
    uint16_t length;
    char* value = nullptr;
public:
    TLV(uint8_t type_, uint16_t length_, std::string value_)
    {
        type = type_; // 0发送，1接收
        length = length_;
        value_ = new char[length];
        std::memcpy(value, value_.c_str(), length);
    }
    void encoder(char* buffer) // 将TLV结构体编码成字节流
    {
        buffer[0] = type;
        buffer[1] = length >> 8;  // 长度高字节
        buffer[2] = length & 0xFF; // 长度低字节
        std::memcpy(buffer + 3, value, length);
    }
    void decoder(const char* buffer) // 解析TLV字节流为TLV结构体
    {
        type = buffer[0];
        length = (buffer[1] << 8) | buffer[2];
        if(value) delete[] value;
        value = new char[length];
        std::memcpy(value, buffer + 3, length);
    }
    ~TLV()
    {
        if(value) delete[] value;
    }
};

class TLVServer {
private:
    int sockfd;
    struct sockaddr_in serverAddr, clientAddr;
    socklen_t addrLen;
    char buffer[1024];

    int _port;
public:
    TLVServer(port = 12345)
    {
        _port = port;

        // 创建UDP套接字
        addrLen = sizeof(clientAddr);
        sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd < 0) {
            std::cerr << "Failed to create socket." << std::endl;
            return 1;
        }

        // 设置服务器地址和端口
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(_port);
        serverAddr.sin_addr.s_addr = INADDR_ANY;

        // 将套接字绑定到服务器地址
        if (bind(sockfd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
            std::cerr << "Failed to bind socket." << std::endl;
            return 1;
        }
    }
    TLVListening()
    {
        while (true) {
            // 接收数据
            memset(buffer, 0, sizeof(buffer));
            ssize_t numBytes = recvfrom(sockfd, buffer, sizeof(buffer), 0, (struct sockaddr*)&clientAddr, &addrLen);
            if (numBytes < 0) {
                std::cerr << "Failed to receive data." << std::endl;
                return 1;
            }

            // 解析收到的TLV数据
            TLV receivedTLV;
            decodeTLV(buffer, receivedTLV);

            // 打印收到的TLV内容
            std::cout << "Received TLV:" << std::endl;
            std::cout << "Type: " << (int)receivedTLV.type << std::endl;
            std::cout << "Length: " << receivedTLV.length << std::endl;
            std::cout << "Value: " << receivedTLV.value << std::endl;

            // 释放内存
            delete[] receivedTLV.value;
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
    TLVClient(const std::string& hostname, port = 12345)
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
    TLVSendMessage(const std::string& message)
    {
        // 发送消息
        ssize_t numBytesSent = sendto(sockfd, message.c_str(), message.length(), 0, res->ai_addr, res->ai_addrlen);
        if (numBytesSent == -1) {
            std::cerr << "sendto error" << std::endl;
            close(sockfd);
            freeaddrinfo(res);
            return;
        }
        std::cout << "Sent " << numBytesSent << " bytes to " << _hostname << ":" << _port << std::endl;
    }
    ~TLVServer()
    {
        close(sockfd);
        freeaddrinfo(res);
    }
};