#include "SSHBind.h"
#include <stdexcept>
#include <fmt/format.h>
#include <Common/Exception.h>
#include <Common/SSH/clibssh.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SSH_EXCEPTION;
}

}


namespace ssh
{

SSHBind::SSHBind() : bind_(ssh_bind_new(), &deleter)
{
    if (!bind_)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to create ssh_bind");
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

void SSHBind::setHostKey(const std::string & key_path)
{
    if (ssh_bind_options_set(bind_.get(), SSH_BIND_OPTIONS_HOSTKEY, key_path.c_str()) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting host key in sshbind due to {}", getError());
}


String SSHBind::getError()
{
    return String(ssh_get_error(bind_.get()));
}

void SSHBind::disableDefaultConfig()
{
    bool enable = false;
    if (ssh_bind_options_set(bind_.get(), SSH_BIND_OPTIONS_PROCESS_CONFIG, &enable) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed disabling default config in sshbind due to {}", getError());
}

void SSHBind::setFd(int fd)
{
    ssh_bind_set_fd(bind_.get(), fd);
}

void SSHBind::listen()
{
    if (ssh_bind_listen(bind_.get()) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed listening in sshbind due to {}", getError());
}

void SSHBind::acceptFd(ssh_session session, int fd)
{
    if (ssh_bind_accept_fd(bind_.get(), session, fd) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed accepting fd in sshbind due to {}", getError());
}

void SSHBind::deleter(ssh_bind bind)
{
    ssh_bind_free(bind);
}

}
