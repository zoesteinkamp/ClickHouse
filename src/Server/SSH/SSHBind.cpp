#include "SSHBind.h"
#include <stdexcept>
#include "clibssh.h"

namespace ssh
{

SSHBind::SSHBind() : bind_(ssh_bind_new(), &deleter)
{
    if (!bind_)
    {
        throw std::runtime_error("Failed to create ssh_bind");
    }
}

SSHBind::~SSHBind() = default;

SSHBind::SSHBind(SSHBind && other) noexcept : bind_(std::move(other.bind_))
{
}

SSHBind & SSHBind::operator=(SSHBind && other) noexcept
{
    if (this != &other)
    {
        bind_ = std::move(other.bind_);
    }
    return *this;
}

ssh_bind SSHBind::get() const
{
    return bind_.get();
}

int SSHBind::setRSAKey(const std::string & rsakey)
{
    return ssh_bind_options_set(bind_.get(), SSH_BIND_OPTIONS_HOSTKEY, rsakey.c_str());
}

String SSHBind::getError()
{
    return String(ssh_get_error(bind_.get()));
}

int SSHBind::setECDSAKey(const std::string & ecdsakey)
{
    return ssh_bind_options_set(bind_.get(), SSH_BIND_OPTIONS_HOSTKEY, ecdsakey.c_str());
}

int SSHBind::setED25519Key(const std::string & ed25519key)
{
    return ssh_bind_options_set(bind_.get(), SSH_BIND_OPTIONS_HOSTKEY, ed25519key.c_str());
}

void SSHBind::setFd(int fd)
{
    ssh_bind_set_fd(bind_.get(), fd);
}

int SSHBind::listen()
{
    return ssh_bind_listen(bind_.get());
}

int SSHBind::acceptFd(ssh_session session, int fd)
{
    return ssh_bind_accept_fd(bind_.get(), session, fd);
}

void SSHBind::deleter(ssh_bind bind)
{
    ssh_bind_free(bind);
}

}
