#pragma once

#include <memory>
#include "Server/SSH/SSHSession.h"

struct ssh_event_struct;

namespace ssh
{

class SSHEvent
{
public:
    using EventPtr = ssh_event_struct *;
    using EventCallback = int (*)(int fd, int revents, void * userdata);

    SSHEvent();
    ~SSHEvent();

    SSHEvent(const SSHEvent &) = delete;
    SSHEvent & operator=(const SSHEvent &) = delete;

    SSHEvent(SSHEvent &&) noexcept;
    SSHEvent & operator=(SSHEvent &&) noexcept;

    void addSession(SSHSession & session);
    void removeSession(SSHSession & session);
    void addFd(int fd, int events, EventCallback cb, void * userdata);
    void removeFd(int fd);
    int poll(int timeout);
    int poll();

private:
    static void deleter(EventPtr e);

    std::unique_ptr<ssh_event_struct, decltype(&deleter)> event;
};

}
