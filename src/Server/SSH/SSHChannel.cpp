#include "SSHChannel.h"
#include <stdexcept>
#include <Common/SSH/clibssh.h>

namespace ssh
{

SSHChannel::SSHChannel(ssh_session session) : channel_(ssh_channel_new(session), &deleter)
{
    if (!channel_)
    {
        throw std::runtime_error("Failed to create ssh_channel");
    }
}

SSHChannel::~SSHChannel() = default;

SSHChannel::SSHChannel(SSHChannel && other) noexcept : channel_(std::move(other.channel_))
{
}

SSHChannel & SSHChannel::operator=(SSHChannel && other) noexcept
{
    if (this != &other)
    {
        channel_ = std::move(other.channel_);
    }
    return *this;
}

ssh_channel SSHChannel::get() const
{
    return channel_.get();
}

int SSHChannel::read(void * dest, uint32_t count, int isStderr)
{
    return ssh_channel_read(channel_.get(), dest, count, isStderr);
}

int SSHChannel::readTimeout(void * dest, uint32_t count, int isStderr, int timeout)
{
    return ssh_channel_read_timeout(channel_.get(), dest, count, isStderr, timeout);
}

int SSHChannel::write(const void * data, uint32_t len)
{
    return ssh_channel_write(channel_.get(), data, len);
}

int SSHChannel::sendEof()
{
    return ssh_channel_send_eof(channel_.get());
}

int SSHChannel::close()
{
    return ssh_channel_close(channel_.get());
}

bool SSHChannel::isOpen()
{
    return ssh_channel_is_open(channel_.get()) != 0;
}

void SSHChannel::deleter(ssh_channel ch)
{
    ssh_channel_free(ch);
}

}
