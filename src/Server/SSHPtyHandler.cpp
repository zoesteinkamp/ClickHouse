#include <atomic>
#include <stdexcept>
#include <Server/EmbeddedClient/openpty.h>
#include <Server/SSH/clibssh.h>
#include <Server/SSHPtyHandler.h>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <sys/poll.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Pipe.h>
#include "Access/Common/AuthenticationType.h"
#include "Access/Credentials.h"
#include "Access/SSHPublicKey.h"
#include "Core/Names.h"
#include "Server/EmbeddedClient/EmbeddedClientRunner.h"
#include "Server/EmbeddedClient/IClientDescriptorSet.h"
#include "Server/EmbeddedClient/PtyClientDescriptorSet.h"
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
    using DescriptorSet = IClientDescriptorSet::DescriptorSet;

    explicit ChannelCallback(ssh::SSHChannel && channel_, std::unique_ptr<Session> && dbSession_)
        : channel(std::move(channel_)), dbSession(std::move(dbSession_))
    {
        channel_cb.userdata = this;
        channel_cb.channel_pty_request_function = pty_request_adapter<ssh_session, ssh_channel, const char *, int, int, int, int>;
        channel_cb.channel_shell_request_function = shell_request_adapter<ssh_session, ssh_channel>;
        channel_cb.channel_data_function = data_function_adapter<ssh_session, ssh_channel, void *, uint32_t, int>;
        channel_cb.channel_pty_window_change_function = pty_resize_adapter<ssh_session, ssh_channel, int, int, int, int>;
        channel_cb.channel_env_request_function = env_request_adapter<ssh_session, ssh_channel, const char *, const char*>;
        channel_cb.channel_exec_request_function = exec_request_adapter<ssh_session, ssh_channel, const char *>;
        ssh_callbacks_init(&channel_cb) ssh_set_channel_callbacks(channel.get(), &channel_cb);
    }

    bool hasClientFinished() { return client_runner.has_value() && client_runner->hasFinished(); }


    DescriptorSet client_input_output;
    ssh::SSHChannel channel;
    std::unique_ptr<Session> dbSession;
    NameToNameMap env;
    std::optional<EmbeddedClientRunner> client_runner;

private:
    int pty_request(ssh_session, ssh_channel, const char * term, int width, int height, int width_pixels, int height_pixels) noexcept
    {
        if (!dbSession || client_runner.has_value())
            return SSH_ERROR;
        try
        {
            auto client_descriptors = std::make_unique<PtyClientDescriptorSet>(String(term), width, height, width_pixels, height_pixels);
            client_runner.emplace(std::move(client_descriptors), std::move(dbSession));
        }
        catch (...)
        {
            return SSH_ERROR;
        }

        return SSH_OK;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, pty_request, int)

    int pty_resize(ssh_session, ssh_channel, int width, int height, int width_pixels, int height_pixels) noexcept
    {
        if (!client_runner.has_value() || !client_runner->hasPty())
        {
            return SSH_ERROR;
        }

        try
        {
            client_runner->changeWindowSize(width, height, width_pixels, height_pixels);
            return SSH_OK;
        }
        catch (...)
        {
            return SSH_ERROR;
        }
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, pty_resize, int)

    int data_function(ssh_session, ssh_channel, void * data, uint32_t len, int is_stderr) const noexcept
    {
        (void)is_stderr;

        if (len == 0 || client_input_output.in == -1)
        {
            return 0;
        }

        return static_cast<int>(write(client_input_output.in, data, len));
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, data_function, int)

    int shell_request(ssh_session, ssh_channel) noexcept
    {
        if (!client_runner.has_value() || client_runner->hasStarted() || !client_runner->hasPty())
        {
            return SSH_ERROR;
        }

        try
        {
            client_runner->run(env);
            client_input_output = client_runner->getDescriptorsForServer();
            return SSH_OK;
        }
        catch (...)
        {
            return SSH_ERROR;
        }
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, shell_request, int)

    int env_request(ssh_session, ssh_channel, const char * env_name, const char * env_value)
    {
        env[env_name] = env_value;
        return SSH_OK;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, env_request, int)

    int exec_nopty(const String & command)
    {
        if (dbSession)
        {
            try
            {
                auto client_descriptors = std::make_unique<PipeClientDescriptorSet>();
                client_runner.emplace(std::move(client_descriptors), std::move(dbSession));
                client_runner->run(env, command);
                client_input_output = client_runner->getDescriptorsForServer();
            }
            catch (...)
            {
                return SSH_ERROR;
            }
        }
        return SSH_OK;
    }

    int exec_request(ssh_session, ssh_channel, const char * command)
    {
        if (client_runner.has_value() && (client_runner->hasStarted() || !client_runner->hasPty()))
        {
            return SSH_ERROR;
        }
        if (client_runner.has_value())
        {
            try
            {
                client_runner->run(env, command);
                client_input_output = client_runner->getDescriptorsForServer();
                return SSH_OK;
            }
            catch (...)
            {
                return SSH_ERROR;
            }
        }
        return exec_nopty(String(command));
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, exec_request, int)


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

int process_stderr(socket_t fd, int revents, void * userdata)
{
    char buf[1024];
    int n = -1;
    ssh_channel channel = static_cast<ssh_channel>(userdata);

    if (channel != nullptr && (revents & POLLIN) != 0) {
        n = static_cast<int>(read(fd, buf, 1024));
        if (n > 0) {
            ssh_channel_write_stderr(channel, buf, n);
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

        /* If child process's stdout/stderr has been registered with the event,
         * or the child process hasn't started yet, continue. */
        if (fdsSet || sdata.channelCallback->client_input_output.out == -1)
        {
            continue;
        }
        /* Executed only once, once the child process starts. */
        fdsSet = true;

        /* If stdout valid, add stdout to be monitored by the poll event. */
        if (sdata.channelCallback->client_input_output.out != -1)
        {
            if (event.add_fd(sdata.channelCallback->client_input_output.out, POLLIN, process_stdout, sdata.channelCallback->channel.get()) != SSH_OK)
            {
                std::cerr << "Failed to register stdout to poll context\n";
                sdata.channelCallback->channel.close();
            }
        }
        if (sdata.channelCallback->client_input_output.err != -1)
        {
            if (event.add_fd(sdata.channelCallback->client_input_output.err, POLLIN, process_stderr, sdata.channelCallback->channel.get()) != SSH_OK)
            {
                std::cerr << "Failed to register stderr to poll context\n";
                sdata.channelCallback->channel.close();
            }
        }
    } while (sdata.channelCallback->channel.isOpen() && !sdata.channelCallback->hasClientFinished() && !server.isCancelled());
    std::cout << "exiting from loop\n";
    std::cout << "Channel open: " << sdata.channelCallback->channel.isOpen() << " finished: " << sdata.channelCallback->hasClientFinished()
              << " server cancelled: " << server.isCancelled() << "\n";


    if (sdata.channelCallback->client_input_output.out != -1)
        event.remove_fd(sdata.channelCallback->client_input_output.out);
    if (sdata.channelCallback->client_input_output.err != -1)
        event.remove_fd(sdata.channelCallback->client_input_output.err);


    sdata.channelCallback->channel.sendEof();
    sdata.channelCallback->channel.close();
    /* Wait up to 5 seconds for the client to terminate the session. */
    for (n = 0; n < 50 && !session.hasFinished(); n++)
    {
        event.poll(100);
    }
    std::cout << "terminating client\n";
}

}
