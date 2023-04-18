#include <atomic>
#include <Server/PTY/openpty.h>
#include <Server/SSH/clibssh.h>
#include <Server/SSHPtyHandler.h>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <sys/poll.h>
#include <Poco/Net/StreamSocket.h>
#include "Common/ThreadPool.h"
#include "Client/LocalServerPty.h"
#include "Server/SSH/SSHChannel.h"
#include "Server/SSH/SSHEvent.h"
#include "Server/TCPServer.h"

namespace DB
{

namespace
{


/*
Need to generate adapter functions, such for each member function, for example:

class SessionCallback {
For this:
    ssh_channel channel_open(ssh_session session) {
        channel = SSHChannel(session);
        return channel->get();
    }


Generate this:
    static ssh_channel channel_open_adapter(ssh_session session, void * userdata) {
        auto * self = static_cast<SessionCallback*>(userdata);
        return self->channel_open;
    }
}

We just static cast userdata to our class and then call member function.
This is needed to use c++ classes in libssh callbacks.
Maybe there is a better way? Or just write boilerplate code and avoid macros?
*/
#define GENERATE_ADAPTER_FUNCTION(class, func_name, return_type) \
    template <typename... Args> \
    static return_type func_name##_adapter(Args... args, void * userdata) \
    { \
        auto * self = static_cast<class *>(userdata); \
        return self->func_name(args...); \
    }


class SessionCallback
{
public:
    explicit SessionCallback(ssh::SSHSession & session)
    {
        server_cb.userdata = this;
        server_cb.auth_pubkey_function = auth_publickey_adapter<ssh_session, const char *, ssh_key, char>;
        ssh_set_auth_methods(session.get(), SSH_AUTH_METHOD_PUBLICKEY);
        server_cb.channel_open_request_session_function = channel_open_adapter<ssh_session>;
        ssh_callbacks_init(&server_cb) ssh_set_server_callbacks(session.get(), &server_cb);
    }

    ssh::SSHChannel & getChannel() { return channel.value(); }

    size_t auth_attempts = 0;
    bool authenticated = false;
    std::optional<ssh::SSHChannel> channel;

private:
    ssh_channel channel_open(ssh_session session)
    {
        channel = ssh::SSHChannel(session);
        return channel->get();
    }

    GENERATE_ADAPTER_FUNCTION(SessionCallback, channel_open, ssh_channel)

    int auth_publickey(ssh_session session, const char * user, ssh_key, char signature_state)
    {
        (void)user;
        (void)session;

        if (signature_state == SSH_PUBLICKEY_STATE_NONE)
        {
            return SSH_AUTH_SUCCESS;
        }

        if (signature_state != SSH_PUBLICKEY_STATE_VALID)
        {
            auth_attempts += 1;
            return SSH_AUTH_DENIED;
        }


        authenticated = true;
        return SSH_AUTH_SUCCESS;
    }

    GENERATE_ADAPTER_FUNCTION(SessionCallback, auth_publickey, int)

    ssh_server_callbacks_struct server_cb = {};
};

class ChannelCallback
{
public:
    explicit ChannelCallback(ssh::SSHChannel & channel, IServer & server_) : server(server_)
    {
        channel_cb.userdata = this;
        channel_cb.channel_pty_request_function = pty_request_adapter<ssh_session, ssh_channel, const char *, int, int, int, int>;
        channel_cb.channel_shell_request_function = shell_request_adapter<ssh_session, ssh_channel>;
        channel_cb.channel_data_function = data_function_adapter<ssh_session, ssh_channel, void *, uint32_t, int>;
        channel_cb.channel_pty_window_change_function = pty_resize_adapter<ssh_session, ssh_channel, int, int, int, int>;
        ssh_callbacks_init(&channel_cb) ssh_set_channel_callbacks(channel.get(), &channel_cb);
    }

    ~ChannelCallback()
    {
        if (threadG.joinable())
        {
            threadG.join();
        }
    }

    int pty_master = -1;
    int pty_slave = -1;
    char pty_slave_name[256];
    int child_stdin = -1;
    int child_stdout = -1;
    winsize winsize = {0, 0, 0, 0};
    DB::IServer & server;
    ThreadFromGlobalPool threadG;
    std::atomic_flag finished = ATOMIC_FLAG_INIT;

private:
    int pty_request(ssh_session session, ssh_channel channel, const char * term, int cols, int rows, int py, int px)
    {
        std::cout << "pty_request\n";

        (void)session;
        (void)channel;
        (void)term;

        winsize.ws_row = rows;
        winsize.ws_col = cols;
        winsize.ws_xpixel = px;
        winsize.ws_ypixel = py;

        if (openpty(&pty_master, &pty_slave, pty_slave_name, nullptr, &winsize) != 0)
        {
            std::cerr << "Failed to open pty\n";
            return SSH_ERROR;
        }
        // disable signals from tty
        struct termios tios;
        if (tcgetattr(pty_slave, &tios) == -1)
        {
            std::cout << "Error: Unable to get termios attributes" << std::endl;
            return SSH_ERROR;
        }
        tios.c_lflag &= ~ISIG;
        if (tcsetattr(pty_slave, TCSANOW, &tios) == -1)
        {
            std::cout << "Error: Unable to set termios attributes" << std::endl;
            return SSH_ERROR;
        }
        // maybe initialize client here
        return SSH_OK;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, pty_request, int)

    int pty_resize(ssh_session, ssh_channel, int cols, int rows, int py, int px)
    {
        winsize.ws_row = rows;
        winsize.ws_col = cols;
        winsize.ws_xpixel = px;
        winsize.ws_ypixel = py;

        if (pty_master != -1)
        {
            return ioctl(pty_master, TIOCSWINSZ, &winsize);
        }

        return SSH_ERROR;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, pty_resize, int)

    int data_function(ssh_session session, ssh_channel channel, void * data, uint32_t len, int is_stderr) const
    {
        (void)session;
        (void)channel;
        (void)is_stderr;

        if (len == 0 || child_stdin == -1)
        {
            return 0;
        }

        return static_cast<int>(write(child_stdin, data, len));
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, data_function, int)

    void shell_execution()
    {
        try
        {
            // Create file_descriptor_source and file_descriptor_sink objects
            boost::iostreams::file_descriptor_source fd_source(pty_slave, boost::iostreams::never_close_handle);
            boost::iostreams::file_descriptor_sink fd_sink(pty_slave, boost::iostreams::never_close_handle);
            // Create a boost::iostreams::stream object using the source and sink
            boost::iostreams::stream<boost::iostreams::file_descriptor_source> tty_input_stream(fd_source);
            boost::iostreams::stream<boost::iostreams::file_descriptor_sink> tty_output_stream(fd_sink);
            tty_output_stream << std::unitbuf;
            std::cout << "construct locaclserver\n";
            auto local = DB::LocalServerPty(server, std::string(pty_slave_name), pty_slave, tty_input_stream, tty_output_stream);
            std::cout << "init locaclserver\n";
            local.init();
            std::cout << "launch locaclserver\n";
            local.main({});
        }
        catch (...)
        {
            std::cerr << "Some Error in client\n";
        }
        std::cout << "exiting...\n";
        finished.test_and_set();
        char c = 0;
        // wake up polling side
        write(pty_slave, &c, 1);
        if (close(pty_slave) == 0)
        {
            std::cout << "slave was closed\n";
        }
    }

    int shell_request(ssh_session session, ssh_channel channel)
    {
        (void)session;
        (void)channel;

        if (child_stdout > 0)
        {
            return SSH_ERROR;
        }

        if (pty_master != -1 && pty_slave != -1)
        {
            threadG = ThreadFromGlobalPool(&ChannelCallback::shell_execution, this);
            /* pty fd is bi-directional */
            child_stdout = child_stdin = pty_master;
            return SSH_OK;
        }
        return SSH_ERROR;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, shell_request, int)


    ssh_channel_callbacks_struct channel_cb = {};
};

int process_stdout(socket_t fd, int revents, void * userdata)
{
    char buf[1024];
    int n = -1;
    ssh_channel channel = static_cast<ssh_channel>(userdata);

    if (channel != nullptr && (revents & POLLIN) != 0)
    {
        n = static_cast<int>(read(fd, buf, 1024));
        if (n > 0)
        {
            ssh_channel_write(channel, buf, n);
        }
    }

    return n;
}

}

SSHPtyHandler::SSHPtyHandler(IServer & server_, ssh::SSHSession && session_, const Poco::Net::StreamSocket & socket)
    : Poco::Net::TCPServerConnection(socket), server(server_), session(std::move(session_))
{
}

void SSHPtyHandler::run()
{
    ssh::SSHEvent event;
    SessionCallback sdata(session);
    // handle_session(session, server);
    if (session.handleKeyExchange() != SSH_OK)
    {
        printf("error\n");
        std::cerr << session.getError() << '\n';
        return;
    }
    event.add_session(session.get());
    int n = 0;
    while (!sdata.authenticated || !sdata.channel.has_value())
    {
        /* If the user has used up all attempts, or if he hasn't been able to
         * authenticate in 10 seconds (n * 100ms), disconnect. */
        if (sdata.auth_attempts >= 3 || n >= 100)
        {
            return;
        }

        if (event.poll(100) == SSH_ERROR)
        {
            std::cout << "ERROR\n";
            std::cerr << session.getError() << '\n';
            return;
        }
        n++;
    }
    ChannelCallback cdata(sdata.getChannel(), server);
    bool fdsSet = false;

    do
    {
        /* Poll the main event which takes care of the session, the channel and
         * even our child process's stdout/stderr (once it's started). */
        if (event.poll(100) == SSH_ERROR)
        {
            sdata.channel->close();
        }
        // std::cout<< "Wake up\n";

        /* If child process's stdout/stderr has been registered with the event,
         * or the child process hasn't started yet, continue. */
        if (fdsSet || cdata.child_stdout == -1)
        {
            continue;
        }
        /* Executed only once, once the child process starts. */
        fdsSet = true;
        /* If stdout valid, add stdout to be monitored by the poll event. */
        if (cdata.child_stdout != -1)
        {
            if (event.add_fd(cdata.child_stdout, POLLIN, process_stdout, sdata.channel->get()) != SSH_OK)
            {
                std::cerr << "Failed to register stdout to poll context\n";
                sdata.channel->close();
            }
        }
    } while (sdata.channel->isOpen() && !cdata.finished.test() && !server.isCancelled());
    std::cout << "exiting from loop\n";
    std::cout << "Channel open: " << sdata.channel->isOpen() << " finished: " << cdata.finished.test()
              << " server cancelled: " << server.isCancelled() << "\n";

    close(cdata.pty_master);

    event.remove_fd(cdata.child_stdout);


    sdata.channel->sendEof();
    sdata.channel->close();

    /* Wait up to 5 seconds for the client to terminate the session. */
    for (n = 0; n < 50 && !session.hasFinished(); n++)
    {
        event.poll(100);
    }
}

}
