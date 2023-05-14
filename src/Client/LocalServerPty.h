#pragma once

#include <Client/ClientCore.h>
#include <Client/LocalConnection.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Loggers/Loggers.h>
#include <Common/InterruptListener.h>
#include <Common/StatusFile.h>

#include <filesystem>
#include <memory>
#include <optional>


namespace DB
{

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServerPty : public ClientCore
{
public:
    explicit LocalServerPty(
        std::unique_ptr<Session> && session_,
        int in_fd_,
        int out_fd_,
        int err_fd_,
        std::istream & iStream,
        std::ostream & oStream,
        std::ostream & eStream)
        : ClientCore(in_fd_, out_fd_, err_fd_, iStream, oStream, eStream), session(std::move(session_))
    {
        global_context = session->makeSessionContext();
    }

    int run(const NameToNameMap & envVars, const String & first_query);

    ~LocalServerPty() override { cleanup(); }

protected:
    void connect() override;

    void processError(const String & query) const override;

    String getName() const override { return "local"; }

private:
    void cleanup();

    std::unique_ptr<Session> session;
};

}
