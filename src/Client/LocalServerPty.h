#pragma once

#include <Client/ClientBasePty.h>
#include <Client/LocalConnection.h>

#include <Common/StatusFile.h>
#include <Common/InterruptListener.h>
#include "Server/IServer.h"
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
class LocalServerPty : public ClientBasePty, public Loggers
{
public:
    explicit LocalServerPty(
        std::unique_ptr<Session> && session_,
        const std::string & tty_file_name_,
        int ttyFd_,
        std::istream & iStream,
        std::ostream & oStream)
        : ClientBasePty(tty_file_name_, ttyFd_, iStream, oStream), session(std::move(session_))
    {
        global_context = session->makeSessionContext();
    }

    int main(const std::vector<String> & /*args*/);


    ~LocalServerPty() override { cleanup(); }

protected:
    void connect() override;

    void processError(const String & query) const override; // TODO

    String getName() const override { return "local"; }

    void printHelpMessage(const OptionsDescription & options_description) override;

    void addOptions(OptionsDescription & options_description) override;

    void processOptions(const OptionsDescription & options_description, const CommandLineOptions & options,
                        const std::vector<Arguments> &, const std::vector<Arguments> &) override;

    void processConfig() override;
    void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &, std::vector<Arguments> &) override;


    void updateLoggerLevel(const String & logs_level) override;

private:
    void cleanup();

    std::unique_ptr<Session> session;
};

}
