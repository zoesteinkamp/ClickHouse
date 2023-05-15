#pragma once

#include <Server/EmbeddedClient/IClientDescriptorSet.h>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <Poco/Pipe.h>
#include "base/types.h"

namespace DB
{


class PtyClientDescriptorSet : public IClientDescriptorSet
{
public:
    PtyClientDescriptorSet(const String & term_name, int width, int height, int width_pixels, int height_pixels);

    DescriptorSet getDescriptorsForClient() override
    {
        return DescriptorSet{.in = pty_slave.get(), .out = pty_slave.get(), .err = pty_slave.get()};
    }

    DescriptorSet getDescriptorsForServer() override { return DescriptorSet{.in = pty_master.get(), .out = pty_master.get(), .err = -1}; }

    StreamSet getStreamsForClient() override { return StreamSet{.in = input_stream, .out = output_stream, .err = output_stream}; }

    void changeWindowSize(int width, int height, int width_pixels, int height_pixels) const;

    void closeServerDescriptors() override { pty_master.safeClose(); }

    bool isPty() const override { return true; }

    ~PtyClientDescriptorSet() override;

private:
    class FileDescriptorWrapper
    {
    public:
        FileDescriptorWrapper() = default;

        void capture(int fd_)
        {
            safeClose();
            fd = fd_;
        }

        int get() const { return fd; }

        void safeClose()
        {
            if (fd != -1)
                if (close(fd) != 0)
                    abort();
            fd = -1;
        }

        ~FileDescriptorWrapper() { safeClose(); }

    private:
        int fd = -1;
    };

    String term_name;
    FileDescriptorWrapper pty_master;
    FileDescriptorWrapper pty_slave;

    // Provide streams on top of file descriptors
    boost::iostreams::file_descriptor_source fd_source; // handles pty_slave lifetime
    boost::iostreams::file_descriptor_sink fd_sink;
    boost::iostreams::stream<boost::iostreams::file_descriptor_source> input_stream;
    boost::iostreams::stream<boost::iostreams::file_descriptor_sink> output_stream;
};

}
