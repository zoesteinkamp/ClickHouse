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

namespace
{

String getEnvOption(const NameToNameMap & envVars, const String & key, const String & defaultvalue)
{
    auto it = envVars.find(key);
    return it == envVars.end() ? defaultvalue : it->second;
}


[[maybe_unused]] Int64 getEnvOptionInt64(const NameToNameMap & envVars, const String & key, Int64 defaultvalue)
{
    auto raw = getEnvOption(envVars, key, "");
    return raw.empty() ? defaultvalue : std::stoll(raw);
}


UInt64 getEnvOptionUInt64(const NameToNameMap & envVars, const String & key, UInt64 defaultvalue)
{
    auto raw = getEnvOption(envVars, key, "");
    return raw.empty() ? defaultvalue : std::stoull(raw);
}


int getEnvOptionInt(const NameToNameMap & envVars, const String & key, int defaultvalue)
{
    auto raw = getEnvOption(envVars, key, "");
    return raw.empty() ? defaultvalue : std::stoi(raw);
}


[[maybe_unused]] unsigned int getEnvOptionUInt(const NameToNameMap & envVars, const String & key, unsigned int defaultvalue)
{
    auto raw = getEnvOption(envVars, key, "");
    return raw.empty() ? defaultvalue : static_cast<unsigned int>(std::stoul(raw));
}


bool getEnvOptionBool(const NameToNameMap & envVars, const String & key, bool defaultvalue)
{
    auto raw = getEnvOption(envVars, key, "");
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


int LocalServerPty::run(const NameToNameMap & envVars, const String & first_query)
{
try
{
    setThreadName("LocalServerPty");
    thread_status.emplace();

    print_stack_trace = getEnvOptionBool(envVars, "stacktrace", false);

    outputStream << std::fixed << std::setprecision(3);
    errorStream << std::fixed << std::setprecision(3);

    is_interactive = stdin_is_a_tty;
    static_query = first_query.empty() ? getEnvOption(envVars, "query", "") : first_query;
    delayed_interactive = is_interactive && !static_query.empty();
    if (!is_interactive || delayed_interactive)
    {
        echo_queries = getEnvOptionBool(envVars, "echo", false) || getEnvOptionBool(envVars, "verbose", false);
        ignore_error = getEnvOptionBool(envVars, "ignore_error", false);
        is_multiquery = true;
    }
    load_suggestions = (is_interactive || delayed_interactive) && !getEnvOptionBool(envVars, "disable_suggestion", false);
    if (load_suggestions)
    {
        suggestion_limit = getEnvOptionInt(envVars, "suggestion_limit", 10000);
    }


    enable_highlight = getEnvOptionBool(envVars, "highlight", true);
    multiline = getEnvOptionBool(envVars, "multiline", false);

    default_database = getEnvOption(envVars, "database", "");


    format = getEnvOption(envVars, "output-format", getEnvOption(envVars, "format", is_interactive ? "PrettyCompact" : "TSV"));
    insert_format = "Values";
    insert_format_max_block_size = getEnvOptionUInt64(envVars, "insert_format_max_block_size",
        global_context->getSettingsRef().max_insert_block_size);



    server_display_name = getEnvOption(envVars, "display_name", getFQDNOrHostName());
    prompt_by_server_display_name = getEnvOption(envVars, "prompt_by_server_display_name", "{display_name} :) ");
    std::map<String, String> prompt_substitutions{{"display_name", server_display_name}};
    for (const auto & [key, value] : prompt_substitutions)
        boost::replace_all(prompt_by_server_display_name, "{" + key + "}", value);
    initTtyBuffer(toProgressOption(getEnvOption(envVars, "progress", "default")));

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
catch (const DB::Exception & e)
{
    cleanup();

    errorStream << getExceptionMessage(e, print_stack_trace, true) << std::endl;
    return e.code() ? e.code() : -1;
}
catch (...)
{
    cleanup();

    errorStream << getCurrentExceptionMessage(false) << std::endl;
    return getCurrentExceptionCode();
}
}


}
