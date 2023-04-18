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

    int setRSAKey(const std::string & rsakey);
    int setECDSAKey(const std::string & ecdsakey);
    int setED25519Key(const std::string & ed25519key);
    void setFd(int fd);
    int listen();
    int acceptFd(ssh_session session, int fd);
    String getError();

private:
    static void deleter(ssh_bind bind);

    std::unique_ptr<ssh_bind_struct, decltype(&deleter)> bind_;
};

}
