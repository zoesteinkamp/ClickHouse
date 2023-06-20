#pragma once

#include <memory>
#include "base/types.h"

struct ssh_session_struct;

namespace ssh
{


class SSHSession
{
public:
    using SessionPtr = ssh_session_struct *;

    SSHSession();
    ~SSHSession();

    SSHSession(const SSHSession &) = delete;
    SSHSession & operator=(const SSHSession &) = delete;

    SSHSession(SSHSession &&) noexcept;
    SSHSession & operator=(SSHSession &&) noexcept;

    SessionPtr getCSessionPtr() const;

    void disableDefaultConfig();
    void connect();
    void setPeerHost(const String & host);
    void setFd(int fd);
    void setTimeout(int timeout, int timeout_usec);
    void disableSocketOwning();
    void handleKeyExchange();
    void disconnect();
    String getError();
    bool hasFinished();

private:
    static void deleter(SessionPtr session);

    std::unique_ptr<ssh_session_struct, decltype(&deleter)> session;
};

}
