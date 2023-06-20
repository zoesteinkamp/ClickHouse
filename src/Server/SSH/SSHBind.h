#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "base/types.h"
#include <Server/SSH/SSHSession.h>

struct ssh_bind_struct;

namespace ssh
{

class SSHBind
{
public:
    using BindPtr = ssh_bind_struct *;

    SSHBind();
    ~SSHBind();

    SSHBind(const SSHBind &) = delete;
    SSHBind & operator=(const SSHBind &) = delete;

    SSHBind(SSHBind &&) noexcept;
    SSHBind & operator=(SSHBind &&) noexcept;

    void disableDefaultConfig();
    void setHostKey(const std::string & key_path);
    void setFd(int fd);
    void listen();
    void acceptFd(SSHSession & session, int fd);
    String getError();

private:
    static void deleter(BindPtr bind);

    std::unique_ptr<ssh_bind_struct, decltype(&deleter)> bind;
};

}
