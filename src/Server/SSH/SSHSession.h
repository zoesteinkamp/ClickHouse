#pragma once

#include <memory>
#include "base/types.h"

struct ssh_session_struct;
using ssh_session = ssh_session_struct *;

namespace ssh
{


class SSHSession
{
public:
    SSHSession();
    ~SSHSession();

    SSHSession(const SSHSession &) = delete;
    SSHSession & operator=(const SSHSession &) = delete;

    SSHSession(SSHSession &&) noexcept;
    SSHSession & operator=(SSHSession &&) noexcept;

    ssh_session get() const;

    int connect();
    void setPeerHost(const String & host);
    void setFd(int fd);
    void setTimeout(int timeout, int timeout_usec);
    int handleKeyExchange();
    void disconnect();
    String getError();
    bool hasFinished();

private:
    static void deleter(ssh_session session);

    std::unique_ptr<ssh_session_struct, decltype(&deleter)> session_;
};

}
