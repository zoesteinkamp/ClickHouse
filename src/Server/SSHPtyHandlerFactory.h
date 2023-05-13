#pragma once
#include <Server/SSHPtyHandler.h>
#include <Server/TCPServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Common/logger_useful.h>
#include "Server/IServer.h"
#include "Server/SSH/LibSSHLogger.h"
#include "Server/SSH/SSHBind.h"
#include "Server/SSH/SSHSession.h"

namespace Poco
{
class Logger;
}
namespace DB
{

class SSHPtyHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;
    ssh::SSHBind bind;

public:
    explicit SSHPtyHandlerFactory(
        IServer & server_, int serverSockFd, const String & rsaKey_, const String & ecdsaKey_, const String & ed25519Key_)
        : server(server_), log(&Poco::Logger::get("SSHHandlerFactory"))
    {
        bind.setRSAKey(rsaKey_);
        bind.setECDSAKey(ecdsaKey_);
        bind.setED25519Key(ed25519Key_);
        bind.setFd(serverSockFd);
        bind.listen();
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer &) override
    {
        LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
        ssh::libsshLogger::initialize();
        ssh::SSHSession session;
        bind.acceptFd(session.get(), socket.sockfd());

        return new SSHPtyHandler(server, std::move(session), socket);
    }
};

}
