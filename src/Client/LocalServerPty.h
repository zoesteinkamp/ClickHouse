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
class LocalServerPty : public ClientCore
{
public:
    explicit LocalServerPty(
        std::unique_ptr<Session> && session_, int in_fd_, int out_fd_, int err_fd_,
        std::istream & iStream, std::ostream & oStream, std::ostream & eStream,
        const NameToNameMap & envVars_, const String & first_query = ""
    )
        : ClientCore(in_fd_, out_fd_, err_fd_, iStream, oStream, eStream), session(std::move(session_)), envVars(envVars_)
    {
        global_context = session->makeSessionContext();
        envVars["query"] = first_query;
    }

    int run();

    ~LocalServerPty() override { cleanup(); }

protected:
    void connect() override;

    void processError(const String & query) const override;

    String getName() const override { return "local"; }

private:
    void cleanup();

    String getEnvOption(const String & key, const String & defaultvalue);

    Int64 getEnvOptionInt64(const String & key, Int64 defaultvalue);

    UInt64 getEnvOptionUInt64(const String & key, UInt64 defaultvalue);

    int getEnvOptionInt(const String & key, int defaultvalue);

    unsigned int getEnvOptionUInt(const String & key, unsigned int defaultvalue);

    bool getEnvOptionBool(const String & key, bool value);

    std::unique_ptr<Session> session;
    NameToNameMap envVars;
};

}
