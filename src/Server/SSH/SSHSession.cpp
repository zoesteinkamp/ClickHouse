#include "SSHSession.h"
#include <stdexcept>
#include <fmt/format.h>
#include "clibssh.h"

namespace ssh
{

SSHSession::SSHSession() : session_(ssh_new(), &deleter)
{
    if (!session_)
    {
        throw std::runtime_error("Failed to create ssh_session");
    }
}

SSHSession::~SSHSession() = default;

SSHSession::SSHSession(SSHSession && other) noexcept : session_(std::move(other.session_))
{
}

SSHSession & SSHSession::operator=(SSHSession && other) noexcept
{
    if (this != &other)
    {
        session_ = std::move(other.session_);
    }
    return *this;
}

ssh_session SSHSession::get() const
{
    return session_.get();
}

void SSHSession::connect()
{
    int rc = ssh_connect(session_.get());
    if (rc != SSH_OK)
    {
        throw std::runtime_error(fmt::format("Failed connecting in ssh session due due to {}", getError()));
    }
}

void SSHSession::disableDefaultConfig()
{
    bool enable = false;
    int rc = ssh_options_set(session_.get(), SSH_OPTIONS_PROCESS_CONFIG, &enable);
    if (rc != SSH_OK)
    {
        throw std::runtime_error(fmt::format("Failed disabling default config for ssh session due due to {}", getError()));
    }
}

void SSHSession::setPeerHost(const String & host)
{
    int rc = ssh_options_set(session_.get(), SSH_OPTIONS_HOST, host.c_str());
    if (rc != SSH_OK)
    {
        throw std::runtime_error(fmt::format("Failed setting peer host option for ssh session due due to {}", getError()));
    }
}

void SSHSession::setFd(int fd)
{
    int rc = ssh_options_set(session_.get(), SSH_OPTIONS_FD, &fd);
    if (rc != SSH_OK)
    {
        throw std::runtime_error(fmt::format("Failed setting fd option for ssh session due due to {}", getError()));
    }
}

void SSHSession::setTimeout(int timeout, int timeout_usec)
{
    int rc = ssh_options_set(session_.get(), SSH_OPTIONS_TIMEOUT, &timeout);
    if (rc != SSH_OK)
    {
        throw std::runtime_error(fmt::format("Failed setting for ssh session due timeout option due to {}", getError()));
    }
    rc |= ssh_options_set(session_.get(), SSH_OPTIONS_TIMEOUT_USEC, &timeout_usec);
    if (rc != SSH_OK)
    {
        throw std::runtime_error(fmt::format("Failed setting for ssh session due timeout_usec option due to {}", getError()));
    }
}

void SSHSession::handleKeyExchange()
{
    int rc = ssh_handle_key_exchange(session_.get());
    if (rc != SSH_OK)
    {
        throw std::runtime_error(fmt::format("Failed key exchange for ssh session due to {}", getError()));
    }
}

void SSHSession::disconnect()
{
    ssh_disconnect(session_.get());
}

String SSHSession::getError()
{
    return String(ssh_get_error(session_.get()));
}

bool SSHSession::hasFinished()
{
    return ssh_get_status(session_.get()) & (SSH_CLOSED | SSH_CLOSED_ERROR);
}

void SSHSession::deleter(ssh_session session)
{
    ssh_free(session);
}

}
