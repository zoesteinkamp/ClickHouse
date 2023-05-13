#include <atomic>
#include <stdexcept>
#include <Server/PTY/openpty.h>
#include <Server/SSH/clibssh.h>
#include <Server/SSHPtyHandler.h>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <sys/poll.h>
#include <Poco/Net/StreamSocket.h>
#include "Common/ThreadPool.h"
#include "Access/Common/AuthenticationType.h"
#include "Access/Credentials.h"
#include "Access/SSHPublicKey.h"
#include "Client/LocalServerPty.h"
#include "Core/Names.h"
#include "Server/SSH/SSHChannel.h"
#include "Server/SSH/SSHEvent.h"

namespace
{

/*
Need to generate adapter functions, such for each member function, for example:

class SessionCallback
{
For this:
    ssh_channel channel_open(ssh_session session)
    {
        channel = SSHChannel(session);
        return channel->get();
    }


Generate this:
    static ssh_channel channel_open_adapter(ssh_session session, void * userdata)
    {
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
}

namespace DB
{

namespace
{


class ChannelCallback
{
public:
    explicit ChannelCallback(ssh::SSHChannel && channel_, std::unique_ptr<Session> && dbSession_)
        : channel(std::move(channel_)), dbSession(std::move(dbSession_))
    {
        channel_cb.userdata = this;
        channel_cb.channel_pty_request_function = pty_request_adapter<ssh_session, ssh_channel, const char *, int, int, int, int>;
        channel_cb.channel_shell_request_function = shell_request_adapter<ssh_session, ssh_channel>;
        channel_cb.channel_data_function = data_function_adapter<ssh_session, ssh_channel, void *, uint32_t, int>;
        channel_cb.channel_pty_window_change_function = pty_resize_adapter<ssh_session, ssh_channel, int, int, int, int>;
        channel_cb.channel_env_request_function = env_request_adapter<ssh_session, ssh_channel, const char *, const char*>;
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
    ssh::SSHChannel channel;
    std::unique_ptr<Session> dbSession;
    ThreadFromGlobalPool threadG;
    std::atomic_flag finished = ATOMIC_FLAG_INIT;
    NameToNameMap env;

private:
    int pty_request(ssh_session, ssh_channel, const char * term, int cols, int rows, int py, int px) noexcept
    {
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
            std::cerr << "Error: Unable to get termios attributes" << std::endl;
            return SSH_ERROR;
        }
        tios.c_lflag &= ~ISIG;
        if (tcsetattr(pty_slave, TCSANOW, &tios) == -1)
        {
            std::cerr << "Error: Unable to set termios attributes" << std::endl;
            return SSH_ERROR;
        }
        // maybe initialize client here
        return SSH_OK;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, pty_request, int)

    int pty_resize(ssh_session, ssh_channel, int cols, int rows, int py, int px) noexcept
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

    int data_function(ssh_session, ssh_channel, void * data, uint32_t len, int is_stderr) const noexcept
    {
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
            auto local = DB::LocalServerPty(std::move(dbSession), pty_slave, tty_input_stream, tty_output_stream, env);
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

    int shell_request(ssh_session, ssh_channel) noexcept
    {
        if (child_stdout > 0)
        {
            return SSH_ERROR;
        }

        if (pty_master != -1 && pty_slave != -1 && dbSession)
        {
            threadG = ThreadFromGlobalPool(&ChannelCallback::shell_execution, this);
            /* pty fd is bi-directional */
            child_stdout = child_stdin = pty_master;
            return SSH_OK;
        }
        return SSH_ERROR;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, shell_request, int)

    int env_request(ssh_session, ssh_channel, const char * env_name, const char * env_value)
    {
        env[env_name] = env_value;
        return SSH_OK;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, env_request, int)


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

class SessionCallback
{
public:
    explicit SessionCallback(ssh::SSHSession & session, IServer & server, const Poco::Net::SocketAddress & address_)
        : server_context(server.context()), peerAddress(address_)
    {
        server_cb.userdata = this;
        server_cb.auth_password_function = auth_password_adapter<ssh_session, const char*, const char*>;
        server_cb.auth_pubkey_function = auth_publickey_adapter<ssh_session, const char *, ssh_key, char>;
        ssh_set_auth_methods(session.get(), SSH_AUTH_METHOD_PASSWORD | SSH_AUTH_METHOD_PUBLICKEY);
        server_cb.channel_open_request_session_function = channel_open_adapter<ssh_session>;
        ssh_callbacks_init(&server_cb) ssh_set_server_callbacks(session.get(), &server_cb);
    }

    size_t auth_attempts = 0;
    bool authenticated = false;
    std::unique_ptr<Session> dbSession;
    DB::ContextMutablePtr server_context;
    Poco::Net::SocketAddress peerAddress;
    std::unique_ptr<ChannelCallback> channelCallback;

private:
    ssh_channel channel_open(ssh_session session) noexcept
    {
        if (!dbSession)
        {
            return nullptr;
        }
        try
        {
            auto channel = ssh::SSHChannel(session);
            channelCallback = std::make_unique<ChannelCallback>(std::move(channel), std::move(dbSession));
            return channelCallback->channel.get();
        }
        catch (const std::runtime_error & err)
        {
            std::cerr << err.what();
            return nullptr;
        }
    }

    GENERATE_ADAPTER_FUNCTION(SessionCallback, channel_open, ssh_channel)

    int auth_password(ssh_session, const char * user, const char * pass) noexcept
    {
        try
        {
            auto dbSessionCreated = std::make_unique<Session>(server_context, ClientInfo::Interface::LOCAL);
            String user_name(user), password(pass);
            dbSessionCreated->authenticate(user_name, password, peerAddress);
            authenticated = true;
            dbSession = std::move(dbSessionCreated);
            return SSH_AUTH_SUCCESS;
        }
        catch (...)
        {
            ++auth_attempts;
            return SSH_AUTH_DENIED;
        }
    }

    GENERATE_ADAPTER_FUNCTION(SessionCallback, auth_password, int)

    int auth_publickey(ssh_session, const char * user, ssh_key key, char signature_state) noexcept
    {
        try
        {
            auto dbSessionCreated = std::make_unique<Session>(server_context, ClientInfo::Interface::LOCAL);
            String user_name(user);


            if (signature_state == SSH_PUBLICKEY_STATE_NONE)
            {
                // This is the case when user wants to check if he is able to use this type of authentication.
                // Also here we may check if the key is assosiated with the user, but current session
                // authentication mechanism doesn't support it.
                if (dbSessionCreated->getAuthenticationType(user_name) != AuthenticationType::SSH_KEY)
                {
                    return SSH_AUTH_DENIED;
                }
                return SSH_AUTH_SUCCESS;
            }

            if (signature_state != SSH_PUBLICKEY_STATE_VALID)
            {
                ++auth_attempts;
                return SSH_AUTH_DENIED;
            }

            // The signature is checked, so just verify that user is assosiated with publickey.
            // Function will throw if authentication fails.
            dbSessionCreated->authenticate(SSHKeyPlainCredentials{user_name, ssh::SSHPublicKey::createNonOwning(key)}, peerAddress);


            authenticated = true;
            dbSession = std::move(dbSessionCreated);
            return SSH_AUTH_SUCCESS;
        }
        catch (...)
        {
            ++auth_attempts;
            return SSH_AUTH_DENIED;
        }
    }

    GENERATE_ADAPTER_FUNCTION(SessionCallback, auth_publickey, int)

    ssh_server_callbacks_struct server_cb = {};
};

}

SSHPtyHandler::SSHPtyHandler(IServer & server_, ssh::SSHSession && session_, const Poco::Net::StreamSocket & socket)
    : Poco::Net::TCPServerConnection(socket), server(server_), session(std::move(session_))
{
}

void SSHPtyHandler::run()
{
    ssh::SSHEvent event;
    SessionCallback sdata(session, server, socket().peerAddress());
    if (session.handleKeyExchange() != SSH_OK)
    {
        printf("error\n");
        std::cerr << session.getError() << '\n';
        return;
    }
    event.add_session(session.get());
    int n = 0;
    while (!sdata.authenticated || !sdata.channelCallback)
    {
        /* If the user has used up all attempts, or if he hasn't been able to
         * authenticate in 10 seconds (n * 100ms), disconnect. */
        if (sdata.auth_attempts >= 3 || n >= 100)
        {
            return;
        }

        if (!server.isCancelled() && event.poll(100) == SSH_ERROR)
        {
            std::cout << "ERROR\n";
            std::cerr << session.getError() << '\n';
            return;
        }
        n++;
    }
    bool fdsSet = false;

    do
    {
        /* Poll the main event which takes care of the session, the channel and
         * even our child process's stdout/stderr (once it's started). */
        if (event.poll(100) == SSH_ERROR)
        {
            sdata.channelCallback->channel.close();
        }
        // std::cout<< "Wake up\n";

        /* If child process's stdout/stderr has been registered with the event,
         * or the child process hasn't started yet, continue. */
        if (fdsSet || sdata.channelCallback->child_stdout == -1)
        {
            continue;
        }
        /* Executed only once, once the child process starts. */
        fdsSet = true;
        /* If stdout valid, add stdout to be monitored by the poll event. */
        if (sdata.channelCallback->child_stdout != -1)
        {
            if (event.add_fd(sdata.channelCallback->child_stdout, POLLIN, process_stdout, sdata.channelCallback->channel.get()) != SSH_OK)
            {
                std::cerr << "Failed to register stdout to poll context\n";
                sdata.channelCallback->channel.close();
            }
        }
    } while (sdata.channelCallback->channel.isOpen() && !sdata.channelCallback->finished.test() && !server.isCancelled());
    std::cout << "exiting from loop\n";
    std::cout << "Channel open: " << sdata.channelCallback->channel.isOpen() << " finished: " << sdata.channelCallback->finished.test()
              << " server cancelled: " << server.isCancelled() << "\n";

    close(sdata.channelCallback->pty_master);

    event.remove_fd(sdata.channelCallback->child_stdout);


    sdata.channelCallback->channel.sendEof();
    sdata.channelCallback->channel.close();

    /* Wait up to 5 seconds for the client to terminate the session. */
    for (n = 0; n < 50 && !session.hasFinished(); n++)
    {
        event.poll(100);
    }
}

}
