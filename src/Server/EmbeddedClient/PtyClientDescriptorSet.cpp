#include <Server/EmbeddedClient/PtyClientDescriptorSet.h>
#include <Common/Exception.h>
#include "openpty.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

PtyClientDescriptorSet::PtyClientDescriptorSet(const String & term_name_, int width, int height, int width_pixels, int height_pixels)
    : term_name(term_name_)
{
    winsize winsize{};
    winsize.ws_col = width;
    winsize.ws_row = height;
    winsize.ws_xpixel = width_pixels;
    winsize.ws_ypixel = height_pixels;
    int pty_master_raw = -1, pty_slave_raw = -1;
    if (openpty(&pty_master_raw, &pty_slave_raw, nullptr, nullptr, &winsize) != 0)
    {
        throwFromErrno("Cannot open pty", ErrorCodes::SYSTEM_ERROR);
    }
    pty_master.capture(pty_master_raw);
    pty_slave.capture(pty_slave_raw);
    fd_source.open(pty_slave.get(), boost::iostreams::never_close_handle);
    fd_sink.open(pty_slave.get(), boost::iostreams::never_close_handle);

    // disable signals from tty
    struct termios tios;
    if (tcgetattr(pty_slave.get(), &tios) == -1)
    {
        throwFromErrno("Cannot get termios from tty via tcgetattr", ErrorCodes::SYSTEM_ERROR);
    }
    tios.c_lflag &= ~ISIG;
    if (tcsetattr(pty_slave.get(), TCSANOW, &tios) == -1)
    {
        throwFromErrno("Cannot set termios to tty via tcsetattr", ErrorCodes::SYSTEM_ERROR);
    }
    input_stream.open(fd_source);
    output_stream.open(fd_sink);
    output_stream << std::unitbuf;
}


void PtyClientDescriptorSet::changeWindowSize(int width, int height, int width_pixels, int height_pixels) const
{
    winsize winsize{};
    winsize.ws_col = width;
    winsize.ws_row = height;
    winsize.ws_xpixel = width_pixels;
    winsize.ws_ypixel = height_pixels;

    if (ioctl(pty_master.get(), TIOCSWINSZ, &winsize) == -1)
    {
        throwFromErrno("Cannot update terminal window size via ioctl TIOCSWINSZ", ErrorCodes::SYSTEM_ERROR);
    }
}


PtyClientDescriptorSet::~PtyClientDescriptorSet() = default;

}
