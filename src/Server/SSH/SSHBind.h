#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "base/types.h"

struct ssh_bind_struct;
using ssh_bind = ssh_bind_struct *;
struct ssh_session_struct;
using ssh_session = ssh_session_struct *;

namespace ssh
{

class SSHBind
{
public:
    SSHBind();
    ~SSHBind();

    SSHBind(const SSHBind &) = delete;
    SSHBind & operator=(const SSHBind &) = delete;

    SSHBind(SSHBind &&) noexcept;
    SSHBind & operator=(SSHBind &&) noexcept;

    ssh_bind get() const;

    void setHostKey(const std::string & key_path);
    void setFd(int fd);
    void listen();
    void acceptFd(ssh_session session, int fd);
    String getError();

private:
    static void deleter(ssh_bind bind);

    std::unique_ptr<ssh_bind_struct, decltype(&deleter)> bind_;
};

}
