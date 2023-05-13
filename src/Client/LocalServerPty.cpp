#include "LocalServerPty.h"

#include <base/getFQDNOrHostName.h>
#include <Interpreters/Session.h>
#include <boost/algorithm/string/replace.hpp>
#include "Common/setThreadName.h"
#include <Common/Exception.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_LOAD_CONFIG;
    extern const int FILE_ALREADY_EXISTS;
}


String LocalServerPty::getEnvOption(const String & key, const String & defaultvalue)
{
    auto it = envVars.find(key);
    return it == envVars.end() ? defaultvalue : it->second;
}


Int64 LocalServerPty::getEnvOptionInt64(const String & key, Int64 defaultvalue)
{
    auto raw = getEnvOption(key, "");
    return raw.empty() ? defaultvalue : std::stoll(raw);
}


UInt64 LocalServerPty::getEnvOptionUInt64(const String & key, UInt64 defaultvalue)
{
    auto raw = getEnvOption(key, "");
    return raw.empty() ? defaultvalue : std::stoull(raw);
}


int LocalServerPty::getEnvOptionInt(const String & key, int defaultvalue)
{
    auto raw = getEnvOption(key, "");
    return raw.empty() ? defaultvalue : std::stoi(raw);
}


unsigned int LocalServerPty::getEnvOptionUInt(const String & key, unsigned int defaultvalue)
{
    auto raw = getEnvOption(key, "");
    return raw.empty() ? defaultvalue : static_cast<unsigned int>(std::stoul(raw));
}


bool LocalServerPty::getEnvOptionBool(const String & key, bool defaultvalue)
{
    auto raw = getEnvOption(key, "");
    if (raw.empty())
    {
        return defaultvalue;
    }
    if (raw == "true" || raw == "1") {
        return true;
    } else if (raw == "false" || raw == "0") {
        return false;
    } else {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad option provided for key {}", key);
    }
}


void LocalServerPty::processError(const String &) const
{
    if (ignore_error)
        return;

    if (is_interactive)
    {
        String message;
        if (server_exception)
        {
            message = getExceptionMessage(*server_exception, print_stack_trace, true);
        }
        else if (client_exception)
        {
            message = client_exception->message();
        }

        errorStream << fmt::format("Received exception\n{}\n\n", message);
    }
    else
    {
        if (server_exception)
            server_exception->rethrow();
        if (client_exception)
            client_exception->rethrow();
    }
}



void LocalServerPty::cleanup()
{
    try
    {
        connection.reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void LocalServerPty::connect()
{
    if (!session)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error creating connection without session object");
    }
    connection_parameters = ConnectionParameters::createForEmbedded(session->sessionContext()->getUserName(), default_database);
    connection = LocalConnection::createConnection(
        connection_parameters, std::move(session), need_render_progress, need_render_profile_events, server_display_name);
    if (!default_database.empty())
    {
        connection->setDefaultDatabase(default_database);
    }
}


int LocalServerPty::main(const std::vector<std::string> & /*args*/)
{
    setThreadName("LocalServerPty");
    thread_status.emplace();

    outputStream << std::fixed << std::setprecision(3);
    errorStream << std::fixed << std::setprecision(3);


    is_interactive = true;
    delayed_interactive = false;
    if (!is_interactive || delayed_interactive)
    {
        echo_queries = getEnvOptionBool("echo", false) || getEnvOptionBool("verbose", false);
        ignore_error = getEnvOptionBool("ignore_error", false);
        is_multiquery = true;
    }
    print_stack_trace = getEnvOptionBool("stacktrace", false);
    load_suggestions = (is_interactive || delayed_interactive) && !getEnvOptionBool("disable_suggestion", false);
    if (load_suggestions)
    {
        suggestion_limit = getEnvOptionInt("suggestion_limit", 10000);
    }

    static_query = getEnvOption("query", "");

    enable_highlight = getEnvOptionBool("highlight", true);
    multiline = getEnvOptionBool("multiline", false);

    default_database = getEnvOption("database", "");


    format = getEnvOption("output-format", getEnvOption("format", is_interactive ? "PrettyCompact" : "TSV"));
    insert_format = "Values";
    insert_format_max_block_size = getEnvOptionUInt64("insert_format_max_block_size",
        global_context->getSettingsRef().max_insert_block_size);



    server_display_name = getEnvOption("display_name", getFQDNOrHostName());
    prompt_by_server_display_name = getEnvOption("prompt_by_server_display_name", "{display_name} :) ");
    std::map<String, String> prompt_substitutions{{"display_name", server_display_name}};
    for (const auto & [key, value] : prompt_substitutions)
        boost::replace_all(prompt_by_server_display_name, "{" + key + "}", value);
    initTtyBuffer(toProgressOption(getEnvOption("progress", "default")));

    if (is_interactive)
    {
        clearTerminal();
        showClientVersion();
        errorStream << std::endl;
    }

    connect();


    if (is_interactive && !delayed_interactive)
    {
        runInteractive();
    }
    else
    {
        runNonInteractive();

        if (delayed_interactive)
            runInteractive();
    }

    cleanup();
    return 0;
}


}
