#include <Databases/DatabaseOnDisk.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/escapeForFileName.h>

#include <common/logger_useful.h>
#include <Poco/DirectoryIterator.h>


#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseAtomic.h>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace DB
{

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;
static constexpr char const * TABLE_WITH_UUID_NAME_PLACEHOLDER = "_";

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_FILE_NAME;
    extern const int SYNTAX_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}


std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & table_data_path_relative,
    Context & context,
    bool has_force_restore_data_flag)
{
    ast_create_query.attach = true;
    ast_create_query.database = database_name;

    if (ast_create_query.as_table_function)
    {
        const auto & table_function = ast_create_query.as_table_function->as<ASTFunction &>();
        const auto & factory = TableFunctionFactory::instance();
        StoragePtr storage = factory.get(table_function.name, context)->execute(ast_create_query.as_table_function, context, ast_create_query.table);
        return {ast_create_query.table, storage};
    }
    /// We do not directly use `InterpreterCreateQuery::execute`, because
    /// - the database has not been loaded yet;
    /// - the code is simpler, since the query is already brought to a suitable form.
    if (!ast_create_query.columns_list || !ast_create_query.columns_list->columns)
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context);
    ConstraintsDescription constraints = InterpreterCreateQuery::getConstraintsDescription(ast_create_query.columns_list->constraints);

    return
    {
        ast_create_query.table,
        StorageFactory::instance().get(
            ast_create_query,
            table_data_path_relative,
            context,
            context.getGlobalContext(),
            columns,
            constraints,
            has_force_restore_data_flag)
    };
}


String getObjectDefinitionFromCreateQuery(const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    auto * create = query_clone->as<ASTCreateQuery>();

    if (!create)
    {
        std::ostringstream query_stream;
        formatAST(*query, query_stream, true);
        throw Exception("Query '" + query_stream.str() + "' is not CREATE query", ErrorCodes::LOGICAL_ERROR);
    }

    if (!create->is_dictionary)
        create->attach = true;

    /// We remove everything that is not needed for ATTACH from the query.
    create->database.clear();
    create->as_database.clear();
    create->as_table.clear();
    create->if_not_exists = false;
    create->is_populate = false;
    create->replace_view = false;

    /// For views it is necessary to save the SELECT query itself, for the rest - on the contrary
    if (!create->is_view && !create->is_materialized_view && !create->is_live_view)
        create->select = nullptr;

    create->format = nullptr;
    create->out_file = nullptr;

    if (!create->uuid.empty())
        create->table = TABLE_WITH_UUID_NAME_PLACEHOLDER;

    std::ostringstream statement_stream;
    formatAST(*create, statement_stream, false);
    statement_stream << '\n';
    return statement_stream.str();
}

DatabaseOnDisk::DatabaseOnDisk(const String & name, const String & metadata_path_, const String & logger, const Context & context_)
    : DatabaseWithOwnTablesBase(name, logger)
    , metadata_path(metadata_path_)
    , data_path("data/" + escapeForFileName(database_name) + "/")
{
    Poco::File(context_.getPath() + getDataPath()).createDirectories();
    Poco::File(getMetadataPath()).createDirectories();
}


void DatabaseOnDisk::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    const auto & settings = context.getSettingsRef();

    /// Create a file with metadata if necessary - if the query is not ATTACH.
    /// Write the query of `ATTACH table` to it.

    /** The code is based on the assumption that all threads share the same order of operations
      * - creating the .sql.tmp file;
      * - adding a table to `tables`;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a table with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.

    if (isDictionaryExist(context, table_name))
        throw Exception("Dictionary " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " already exists.",
            ErrorCodes::DICTIONARY_ALREADY_EXISTS);

    if (isTableExist(context, table_name))
        throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement;

    {
        statement = getObjectDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        /// Add a table to the map of known tables.
        attachTable(table_name, table, getDataPath(query->as<ASTCreateQuery &>()));

        /// If it was ATTACH query and file with table metadata already exist
        /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
        Poco::File(table_metadata_tmp_path).renameTo(table_metadata_path);
    }
    catch (...)
    {
        Poco::File(table_metadata_tmp_path).remove();
        throw;
    }
}

void DatabaseOnDisk::removeTable(const Context & /* context */, const String & table_name)
{
    String table_data = getDataPath(table_name);

    StoragePtr res = detachTable(table_name);

    String table_metadata_path = getObjectMetadataPath(table_name);

    try
    {
        Poco::File(table_metadata_path).remove();
    }
    catch (...)
    {
        try
        {
            Poco::File(table_metadata_path + ".tmp_drop").remove();
            return;
        }
        catch (...)
        {
            LOG_WARNING(log, getCurrentExceptionMessage(__PRETTY_FUNCTION__));
        }
        attachTable(table_name, res, data_path);
        throw;
    }
}

void DatabaseOnDisk::renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name)
{
    bool from_ordinary_to_atomic = false;
    bool from_atomic_to_ordinary = false;
    if (typeid(*this) != typeid(to_database))
    {
        if (typeid_cast<DatabaseOrdinary *>(this) && typeid_cast<DatabaseAtomic *>(&to_database))
            from_ordinary_to_atomic = true;
        else if (typeid_cast<DatabaseAtomic *>(this) && typeid_cast<DatabaseOrdinary *>(&to_database))
            from_atomic_to_ordinary = true;
        else
            throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    StoragePtr table = tryGetTable(context, table_name);

    if (!table)
        throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    auto table_lock = table->lockExclusively(context.getCurrentQueryId());

    ASTPtr ast = parseQueryFromMetadata(getObjectMetadataPath(table_name));
    auto & create = ast->as<ASTCreateQuery &>();
    create.table = to_table_name;
    if (from_ordinary_to_atomic)
        create.uuid = boost::uuids::to_string(boost::uuids::random_generator()());
    if (from_atomic_to_ordinary)
        create.uuid.clear();

    /// Notify the table that it is renamed. If the table does not support renaming, exception is thrown.
    try
    {
        table->rename(to_database.getDataPath(create),
                      to_database.getDatabaseName(),
                      to_table_name, table_lock);
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        /// Better diagnostics.
        throw Exception{Exception::CreateFromPoco, e};
    }

    /// NOTE Non-atomic.
    to_database.createTable(context, to_table_name, table, ast);
    removeTable(context, table_name);
}

ASTPtr DatabaseOnDisk::getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const
{
    ASTPtr ast;

    auto table_metadata_path = getObjectMetadataPath(table_name);
    ast = getCreateQueryFromMetadata(table_metadata_path, throw_on_error);
    if (!ast && throw_on_error)
    {
        /// Handle system.* tables for which there are no table.sql files.
        bool has_table = tryGetTable(context, table_name) != nullptr;

        auto msg = has_table
                   ? "There is no CREATE TABLE query for table "
                   : "There is no metadata file for table ";

        throw Exception(msg + backQuote(table_name), ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
    }

    return ast;
}

ASTPtr DatabaseOnDisk::getCreateDatabaseQuery() const
{
    ASTPtr ast;

    auto database_metadata_path = getDatabaseMetadataPath(getMetadataPath());
    ast = getCreateQueryFromMetadata(database_metadata_path, true);
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        /// If database.sql doesn't exist, then engine is Ordinary
        String query = "CREATE DATABASE " + backQuoteIfNeed(getDatabaseName()) + " ENGINE = Ordinary";
        ParserCreateQuery parser;
        ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0);
    }

    return ast;
}

void DatabaseOnDisk::drop(const Context & context)
{
    Poco::File(context.getPath() + getDataPath()).remove(false);
    Poco::File(getMetadataPath()).remove(false);
}

String DatabaseOnDisk::getObjectMetadataPath(const String & table_name) const
{
    String base_path = getMetadataPath();
    //FIXME
    return base_path + (endsWith(base_path, "/") ? "" : "/") + escapeForFileName(table_name) + ".sql";
}

time_t DatabaseOnDisk::getObjectMetadataModificationTime(const String & table_name) const
{
    String table_metadata_path = getObjectMetadataPath(table_name);
    Poco::File meta_file(table_metadata_path);

    if (meta_file.exists())
        return meta_file.getLastModified().epochTime();
    else
        return static_cast<time_t>(0);
}

void DatabaseOnDisk::iterateMetadataFiles(const Context & /*context*/, const IteratingFunction & iterating_function) const
{
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(getMetadataPath()); dir_it != dir_end; ++dir_it)
    {
        /// For '.svn', '.gitignore' directory and similar.
        if (dir_it.name().at(0) == '.')
            continue;

        /// There are .sql.bak files - skip them.
        if (endsWith(dir_it.name(), ".sql.bak"))
            continue;

        // There are files that we tried to delete previously
        static const char * tmp_drop_ext = ".sql.tmp_drop";
        if (endsWith(dir_it.name(), tmp_drop_ext))
        {
            //const std::string object_name = dir_it.name().substr(0, dir_it.name().size() - strlen(tmp_drop_ext));

            //FIXME
            //if (Poco::File(context.getPath() + getDataPath(object_name)).exists())
            //{
            //    Poco::File(dir_it->path()).renameTo(object_name + ".sql");
            //    LOG_WARNING(log, "Object " << backQuote(object_name) << " was not dropped previously");
            //}
            //else
            //{
            //    LOG_INFO(log, "Removing file " << dir_it->path());
            //    Poco::File(dir_it->path()).remove();
            //}
            continue;
        }

        /// There are files .sql.tmp - delete
        if (endsWith(dir_it.name(), ".sql.tmp"))
        {
            LOG_INFO(log, "Removing file " << dir_it->path());
            Poco::File(dir_it->path()).remove();
            continue;
        }

        /// The required files have names like `table_name.sql`
        if (endsWith(dir_it.name(), ".sql"))
        {
            iterating_function(dir_it.name());
        }
        else
            throw Exception("Incorrect file extension: " + dir_it.name() + " in metadata directory " + getMetadataPath(),
                ErrorCodes::INCORRECT_FILE_NAME);
    }
}

String DatabaseOnDisk::getDatabaseMetadataPath(const String & base_path) const
{
    //FIXME
    return (endsWith(base_path, "/") ? base_path.substr(0, base_path.size() - 1) : base_path) + ".sql";
}

ASTPtr DatabaseOnDisk::parseQueryFromMetadata(const String & metadata_file_path, bool throw_on_error, bool remove_empty) const
{
    String query;

    try
    {
        ReadBufferFromFile in(metadata_file_path, METADATA_FILE_BUFFER_SIZE);
        readStringUntilEOF(query, in);
    }
    catch (const Exception & e)
    {
        if (!throw_on_error && e.code() == ErrorCodes::FILE_DOESNT_EXIST)
            return nullptr;
        else
            throw;
    }

    /** Empty files with metadata are generated after a rough restart of the server.
      * Remove these files to slightly reduce the work of the admins on startup.
      */
    if (remove_empty && query.empty())
    {
        LOG_ERROR(log, "File " << metadata_file_path << " is empty. Removing.");
        Poco::File(metadata_file_path).remove();
        return nullptr;
    }

    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message, /* hilite = */ false,
                             "in file " + metadata_path, /* allow_multi_statements = */ false, 0);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);
    else if (!ast)
        return nullptr;

    auto & create = ast->as<ASTCreateQuery &>();
    if (!create.uuid.empty())
    {
        String table_name = Poco::Path(metadata_file_path).makeFile().getBaseName();
        if (create.table != TABLE_WITH_UUID_NAME_PLACEHOLDER)
            LOG_WARNING(log, "File " << metadata_file_path << " contains both UUID and table name. "
                                                    "Will use name `" << table_name << "` instead of `" << create.table << "`");
        create.table = table_name;
    }

    return ast;
}

ASTPtr DatabaseOnDisk::getCreateQueryFromMetadata(const String & database_metadata_path, bool throw_on_error) const
{
    ASTPtr ast = parseQueryFromMetadata(database_metadata_path, throw_on_error);

    if (ast)
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.attach = false;
        ast_create_query.database = database_name;
    }

    return ast;
}

String DatabaseOnDisk::getDataPath(const String & table_name) const
{
    return data_path + escapeForFileName(table_name) + "/";
}

String DatabaseOnDisk::getDataPath(const ASTCreateQuery & query) const
{
    return getDataPath(query.table);
}

}
