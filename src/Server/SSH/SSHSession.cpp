#include "SSHSession.h"
#include <stdexcept>
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

int SSHSession::connect()
{
    return ssh_connect(session_.get());
}

void SSHSession::setPeerHost(const String & host)
{
    ssh_options_set(session_.get(), SSH_OPTIONS_HOST, host.c_str());
}

void SSHSession::setFd(int fd)
{
    ssh_options_set(session_.get(), SSH_OPTIONS_FD, &fd);
}

void SSHSession::setTimeout(int timeout, int timeout_usec)
{
    ssh_options_set(session_.get(), SSH_OPTIONS_TIMEOUT, &timeout);
    ssh_options_set(session_.get(), SSH_OPTIONS_TIMEOUT_USEC, &timeout_usec);
}

int SSHSession::handleKeyExchange()
{
    return ssh_handle_key_exchange(session_.get());
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
