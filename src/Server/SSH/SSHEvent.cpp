#include "SSHEvent.h"
#include <stdexcept>
#include "clibssh.h"

namespace ssh
{

SSHEvent::SSHEvent() : event_(ssh_event_new(), &deleter)
{
    if (!event_)
    {
        throw std::runtime_error("Failed to create ssh_event");
    }
}

SSHEvent::~SSHEvent() = default;

SSHEvent::SSHEvent(SSHEvent && other) noexcept : event_(std::move(other.event_))
{
}

SSHEvent & SSHEvent::operator=(SSHEvent && other) noexcept
{
    if (this != &other)
    {
        event_ = std::move(other.event_);
    }
    return *this;
}

ssh_event SSHEvent::get() const
{
    return event_.get();
}

void SSHEvent::add_session(ssh_session session)
{
    if (ssh_event_add_session(event_.get(), session) == SSH_ERROR)
        throw std::runtime_error("Error adding session to ssh event");
}

void SSHEvent::remove_session(ssh_session session)
{
    ssh_event_remove_session(event_.get(), session);
}

int SSHEvent::poll(int timeout)
{
    int rc = ssh_event_dopoll(event_.get(), timeout);
    if (rc == SSH_ERROR)
    {
        throw std::runtime_error("Error on polling on ssh event");
    }
    return rc;
}

int SSHEvent::poll()
{
    return poll(-1);
}

void SSHEvent::add_fd(int fd, int events, ssh_event_callback cb, void * userdata)
{
    if (ssh_event_add_fd(event_.get(), fd, events, cb, userdata) == SSH_ERROR)
        throw std::runtime_error("Error on adding custom file descriptor to ssh event");
}

void SSHEvent::remove_fd(socket_t fd)
{
    ssh_event_remove_fd(event_.get(), fd);
}

void SSHEvent::deleter(ssh_event e)
{
    ssh_event_free(e);
}

}
