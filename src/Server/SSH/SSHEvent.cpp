#include "SSHEvent.h"
#include <stdexcept>
#include <Common/SSH/clibssh.h>

namespace ssh
{

SSHEvent::SSHEvent() : event(ssh_event_new(), &deleter)
{
    if (!event)
    {
        throw std::runtime_error("Failed to create ssh_event");
    }
}

SSHEvent::~SSHEvent() = default;

SSHEvent::SSHEvent(SSHEvent && other) noexcept : event(std::move(other.event))
{
}

SSHEvent & SSHEvent::operator=(SSHEvent && other) noexcept
{
    if (this != &other)
    {
        event = std::move(other.event);
    }
    return *this;
}

ssh_event SSHEvent::get() const
{
    return event.get();
}

void SSHEvent::addSession(ssh_session session)
{
    if (ssh_event_add_session(event.get(), session) == SSH_ERROR)
        throw std::runtime_error("Error adding session to ssh event");
}

void SSHEvent::removeSession(ssh_session session)
{
    ssh_event_remove_session(event.get(), session);
}

int SSHEvent::poll(int timeout)
{
    int rc = ssh_event_dopoll(event.get(), timeout);
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

void SSHEvent::addFd(int fd, int events, ssh_event_callback cb, void * userdata)
{
    if (ssh_event_add_fd(event.get(), fd, events, cb, userdata) == SSH_ERROR)
        throw std::runtime_error("Error on adding custom file descriptor to ssh event");
}

void SSHEvent::removeFd(socket_t fd)
{
    ssh_event_remove_fd(event.get(), fd);
}

void SSHEvent::deleter(ssh_event e)
{
    ssh_event_free(e);
}

}
