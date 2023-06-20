#pragma once

#include <memory>
#include "Server/SSH/SSHSession.h"

struct ssh_channel_struct;

namespace ssh
{

class SSHChannel
{
public:
    using ChannelPtr = ssh_channel_struct *;

    explicit SSHChannel(SSHSession::SessionPtr session);
    ~SSHChannel();

    SSHChannel(const SSHChannel &) = delete;
    SSHChannel & operator=(const SSHChannel &) = delete;

    SSHChannel(SSHChannel &&) noexcept;
    SSHChannel & operator=(SSHChannel &&) noexcept;

    ChannelPtr getCChannelPtr() const;

    int read(void * dest, uint32_t count, int isStderr);
    int readTimeout(void * dest, uint32_t count, int isStderr, int timeout);
    int write(const void * data, uint32_t len);
    int sendEof();
    int close();
    bool isOpen();

private:
    static void deleter(ChannelPtr ch);

    std::unique_ptr<ssh_channel_struct, decltype(&deleter)> channel;
};

}
