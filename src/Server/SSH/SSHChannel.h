#pragma once

#include <memory>

struct ssh_channel_struct;
struct ssh_session_struct;
using ssh_channel = ssh_channel_struct *;
using ssh_session = ssh_session_struct *;

namespace ssh
{

class SSHChannel
{
public:
    explicit SSHChannel(ssh_session session);
    ~SSHChannel();

    SSHChannel(const SSHChannel &) = delete;
    SSHChannel & operator=(const SSHChannel &) = delete;

    SSHChannel(SSHChannel &&) noexcept;
    SSHChannel & operator=(SSHChannel &&) noexcept;

    ssh_channel get() const;

    int read(void * dest, uint32_t count, int isStderr);
    int readTimeout(void * dest, uint32_t count, int isStderr, int timeout);
    int write(const void * data, uint32_t len);
    int sendEof();
    int close();
    bool isOpen();

private:
    static void deleter(ssh_channel ch);

    std::unique_ptr<ssh_channel_struct, decltype(&deleter)> channel_;
};

}
