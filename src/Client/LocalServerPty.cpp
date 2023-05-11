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
    connection_parameters = ConnectionParameters::createForEmbedded(session->sessionContext()->getUserName(), session->sessionContext()->getCurrentDatabase());
    connection = LocalConnection::createConnection(
        connection_parameters, std::move(session), need_render_progress, need_render_profile_events, server_display_name);
}


int LocalServerPty::main(const std::vector<std::string> & /*args*/)
{
    setThreadName("LocalServerPty");
    thread_status.emplace();

    StackTrace::setShowAddresses(false);

    outputStream << std::fixed << std::setprecision(3);
    errorStream << std::fixed << std::setprecision(3);


    is_interactive = true;
    delayed_interactive = false;
    if (!is_interactive || delayed_interactive)
    {
        echo_queries = getConfig().hasOption("echo") || getConfig().hasOption("verbose");
        ignore_error = getConfig().getBool("ignore-error", false);
        is_multiquery = true;
    }
    print_stack_trace = getConfig().getBool("stacktrace", false);
    load_suggestions = (is_interactive || delayed_interactive) && !getConfig().getBool("disable_suggestion", false);


    format = getConfig().getString("output-format", getConfig().getString("format", is_interactive ? "PrettyCompact" : "TSV"));
    insert_format = "Values";

    /// Setting value from cmd arg overrides one from config
    if (global_context->getSettingsRef().max_insert_block_size.changed)
    {
        insert_format_max_block_size = global_context->getSettingsRef().max_insert_block_size;
    }
    else
    {
        insert_format_max_block_size = getConfig().getUInt64("insert_format_max_block_size",
            global_context->getSettingsRef().max_insert_block_size);
    }



    server_display_name = getConfig().getString("display_name", getFQDNOrHostName());
    prompt_by_server_display_name = getConfig().getRawString("prompt_by_server_display_name.default", "{display_name} :) ");
    std::map<String, String> prompt_substitutions{{"display_name", server_display_name}};
    for (const auto & [key, value] : prompt_substitutions)
        boost::replace_all(prompt_by_server_display_name, "{" + key + "}", value);
    initTtyBuffer(toProgressOption(getConfig().getString("progress", "default")));

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

void LocalServerPty::updateLoggerLevel(const String & )
{
}


}
