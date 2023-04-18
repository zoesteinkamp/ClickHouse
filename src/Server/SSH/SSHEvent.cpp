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
    ssh_event_add_session(event_.get(), session);
}

void SSHEvent::remove_session(ssh_session session)
{
    ssh_event_remove_session(event_.get(), session);
}

int SSHEvent::poll(int timeout)
{
    return ssh_event_dopoll(event_.get(), timeout);
}

int SSHEvent::poll()
{
    return poll(-1);
}

int SSHEvent::add_fd(int fd, int events, ssh_event_callback cb, void * userdata)
{
    return ssh_event_add_fd(event_.get(), fd, events, cb, userdata);
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
