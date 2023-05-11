#pragma once

#include <Client/ClientCore.h>
#include <Client/LocalConnection.h>

#include <Common/StatusFile.h>
#include <Common/InterruptListener.h>
#include <Loggers/Loggers.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <filesystem>
#include <memory>
#include <optional>


namespace DB
{

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServerPty : public ClientCore, public Loggers
{
public:
    explicit LocalServerPty(
        std::unique_ptr<Session> && session_,
        const std::string & ,
        int ttyFd_,
        std::istream & iStream,
        std::ostream & oStream)
        : ClientCore(ttyFd_, ttyFd_, ttyFd_, iStream, oStream, oStream), session(std::move(session_))
    {
        setApp();
        global_context = session->makeSessionContext();
    }

    int main(const std::vector<String> & /*args*/);


    ~LocalServerPty() override { cleanup(); }

protected:
    void connect() override;

    void processError(const String & query) const override; // TODO

    String getName() const override { return "local"; }


    void updateLoggerLevel(const String & logs_level) override;

private:
    void cleanup();

    std::unique_ptr<Session> session;
};

}
