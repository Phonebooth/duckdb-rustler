defmodule DatabaseThing do
  @moduledoc """
  DuckDB API module
  """

  #Changes from old API:
  #Replaced library_version/0 with library_version(connection()) (versions are tied to connections in Rust API)
  #Removed storage_format_version (not supported in rust API)
  #Removed library_version(storage_format_version) (not supported in rust API)
  #Removed source_id (not part of rust API)
  #Removed platform (not part of rust API)
  #Removed extension_is_loaded (don't see a way to check for extensions in rust)


  @type db() :: reference()
  @type reason() :: :atom | binary()
  @type connection() :: reference()
  @type statement() :: reference()
  @type query_result() :: reference()
  @type appender :: reference()

  @doc """
  Opens database in the specified file.

  If specified file does not exist, a new database file with the given name will be created automatically.

  ## Examples
  ```
  iex> {:ok, _db} = DatabaseThing.open("my_database.duckdb", %DatabaseThing.Config{})
  ```
  """
  @spec open(binary(), DatabaseThing.Config.t | nil) :: {:ok, db()} | {:error, reason()}
  def open(path, config) when is_binary(path),
    do: DatabaseThing.NIF.open(path, config)

  @doc """
  If the path to the file is specified, then opens the database in the file.

  If specified file does not exist, a new database file with the given name will be created automatically.
  If database config is specified, then opens the database in the memory with the custom database config.

  ## Examples
  ```
  iex> {:ok, _db} = DatabaseThing.open("my_database.duckdb")

  iex> {:ok, _db} = DatabaseThing.open(%DatabaseThing.Config{access_mode: :automatic})
  ```
  """
  @spec open(binary() | DatabaseThing.Config.t) :: {:ok, db()} | {:error, reason()}
  def open(path) when is_binary(path),
    do: DatabaseThing.NIF.open(path, nil)

  def open(%DatabaseThing.Config{} = config),
    do: DatabaseThing.NIF.open(":memory:", config)

  @doc """
  Opens database in the memory.

  ## Examples
  ```
  iex> {:ok, _db} = DatabaseThing.open()
  ```
  """
  @spec open() :: {:ok, db()} | {:error, reason()}
  def open(),
    do: DatabaseThing.NIF.open(":memory:", nil)

  @doc """
  Closes database that has been opened.

  The reference representing the database is no longer usable after close is called. Trying to use it
  will cause undefined behavior, and very likely a SEGFAULT. For this reason, the developer must make sure
  that all agents on the system have removed the database reference from their working memory before close
  is called.

  See http://erlang.org/pipermail/erlang-questions/2020-November/100131.html for a discussion on why you
  might want to call close/1 explicitly instead of relying on the garbage collector.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open("my_database.duckdb")
  iex> DatabaseThing.close(db)
  :ok

  """
  @spec close(db()) :: :ok | {:error, reason()}
  def close(db) when is_reference(db), do: DatabaseThing.NIF.close(db)

  @doc """
  Creates connection object to work with database.

  To work with database the connection object is requiered. Connection object hold a shared reference to database, so it is possible to forget the database reference and hold the connection reference only.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, _conn} = DatabaseThing.connection(db)
  ```
  """
  @spec connection(db()) :: {:ok, connection()} | {:error, reason()}
  def connection(db) when is_reference(db),
    do: DatabaseThing.NIF.connection(db)

  @doc """
  Issues a query to the database and returns a result reference.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _res} = DatabaseThing.query(conn, "SELECT 1;")
  ```
  """
  @spec query(connection(), binary()) :: {:ok, query_result()} | {:error, reason()}
  def query(connection, sql_string) when is_reference(connection) and is_binary(sql_string),
    do: DatabaseThing.NIF.query(connection, sql_string)

  @doc """
  Issues a query to the database with parameters and returns a result reference.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _res} = DatabaseThing.query(conn, "SELECT 1 WHERE $1 = 1;", [1])
  ```
  """
  @spec query(connection(), binary(), list()) :: {:ok, query_result()} | {:error, reason()}
  def query(connection, sql_string, args) when is_reference(connection) and is_binary(sql_string) and is_list(args),
    do: DatabaseThing.NIF.query(connection, sql_string, args)

  @doc """
  Prepare the specified query, returning a reference to the prepared statement object

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _stmt} = DatabaseThing.prepare_statement(conn, "SELECT 1 WHERE $1 = 1;")
  ```
  """
  @spec prepare_statement(connection(), binary()) :: {:ok, statement()} | {:error, reason()}
  def prepare_statement(connection, sql_string) when is_reference(connection) and is_binary(sql_string),
    do: DatabaseThing.NIF.prepare_statement(connection, sql_string)

  @doc """
  Execute the prepared statement

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, stmt} = DatabaseThing.prepare_statement(conn, "SELECT 1;")
  iex> {:ok, res} = DatabaseThing.execute_statement(stmt)
  iex> [[1]] = DatabaseThing.fetch_all(res)
  ```
  """
  @spec execute_statement(statement()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(statement) when is_reference(statement),
    do: DatabaseThing.NIF.execute_statement(statement)

  @doc """
  Execute the prepared statement with the given list of parameters

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, stmt} = DatabaseThing.prepare_statement(conn, "SELECT 1 WHERE $1 = 1;")
  iex> {:ok, res} = DatabaseThing.execute_statement(stmt, [1])
  iex> [[1]] = DatabaseThing.fetch_all(res)
  ```
  """
  @spec execute_statement(statement(), list()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(statement, args) when is_reference(statement) and is_list(args),
    do: DatabaseThing.NIF.execute_statement(statement, args)

  @doc """
  Gets the list of column names from the query result.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, res} = DatabaseThing.query(conn, "SELECT 1 AS n;")
  iex> ["n"] = DatabaseThing.get_column_names(res)
  ```
  """
  @spec get_column_names(query_result()) :: list()
  def get_column_names(query_result) when is_reference(query_result),
    do: DatabaseThing.NIF.get_column_names(query_result)

  @doc """
  Fetches a data chunk from the query result.

  Returns empty list if there are no more results to fetch.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, res} = DatabaseThing.query(conn, "SELECT 1;")
  iex> [[1]] = DatabaseThing.fetch_chunk(res)
  ```
  """
  @spec fetch_chunk(query_result()) :: list()
  def fetch_chunk(query_result) when is_reference(query_result),
    do: DatabaseThing.NIF.fetch_chunk(query_result)


  @doc """
  Fetches all data from the query result.

  Returns empty list if there are no result to fetch.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, res} = DatabaseThing.query(conn, "SELECT 1;")
  iex> [[1]] = DatabaseThing.fetch_all(res)
  ```
  """
  @spec fetch_all(query_result()) :: list()
  def fetch_all(query_result) when is_reference(query_result),
    do: DatabaseThing.NIF.fetch_all(query_result)

  @doc """
  Creates the Appender to load bulk data into a DuckDB database.

  This is the recommended way to load bulk data.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _res} = DatabaseThing.query(conn, "CREATE TABLE table_1 (data INTEGER);")
  iex> {:ok, _appender} = DatabaseThing.appender(conn, "table_1")
  ```
  """
  @spec appender(connection(), binary()) :: {:ok, appender()} | {:error, reason()}
  def appender(connection, table_name) when is_reference(connection) and is_binary(table_name),
    do: DatabaseThing.NIF.appender(connection, table_name)

  @doc """
  Append row into a DuckDB database table.

  Any values added to the appender are cached prior to being inserted into the database system for performance reasons. That means that, while appending, the rows might not be immediately visible in the system. The cache is automatically flushed when the appender goes out of scope or when DatabaseThing.appender_close(appender) is called. The cache can also be manually flushed using the DatabaseThing.appender_flush(appender) method. After either flush or close is called, all the data has been written to the database system.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _res} = DatabaseThing.query(conn, "CREATE TABLE table_1 (data INTEGER);")
  iex> {:ok, appender} = DatabaseThing.appender(conn, "table_1")
  iex> :ok = DatabaseThing.appender_add_row(appender, [1])
  ```
  """
  @spec appender_add_row(appender(), list()) :: :ok | {:error, reason()}
  def appender_add_row(appender, row) when is_reference(appender) and is_list(row),
    do: DatabaseThing.NIF.appender_add_row(appender, row)

  @doc """
  Append multiple rows into a DuckDB database table at once.

  Any values added to the appender are cached prior to being inserted into the database system for performance reasons. That means that, while appending, the rows might not be immediately visible in the system. The cache is automatically flushed when the appender goes out of scope or when `DatabaseThing.appender_close/1` is called. The cache can also be manually flushed using the `DatabaseThing.appender_flush/1` method. After either flush or close is called, all the data has been written to the database system.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _res} = DatabaseThing.query(conn, "CREATE TABLE table_1 (the_n1 INTEGER, the_str1 STRING);")
  iex> {:ok, appender} = DatabaseThing.appender(conn, "table_1")
  iex> :ok = DatabaseThing.appender_add_rows(appender, [[1, "one"], [2, "two"]])
  ```
  """
  @spec appender_add_rows(appender(), list(list())) :: :ok | {:error, reason()}
  def appender_add_rows(appender, rows) when is_reference(appender) and is_list(rows),
    do: DatabaseThing.NIF.appender_add_rows(appender, rows)

  @doc """
  Commit the changes made by the appender.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _res} = DatabaseThing.query(conn, "CREATE TABLE table_1 (the_n1 INTEGER, the_str1 STRING);")
  iex> {:ok, appender} = DatabaseThing.appender(conn, "table_1")
  iex> :ok = DatabaseThing.appender_add_rows(appender, [[1, "one"], [2, "two"]])
  iex> {:ok, res} = DatabaseThing.query(conn, "SELECT * FROM table_1;")
  iex> [] = DatabaseThing.fetch_all(res)
  iex> :ok = DatabaseThing.appender_flush(appender)
  iex> {:ok, res} = DatabaseThing.query(conn, "SELECT * FROM table_1;")
  iex> [[1, "one"], [2, "two"]] = DatabaseThing.fetch_all(res)
  ```
  """
  @spec appender_flush(appender()) :: :ok | {:error, reason()}
  def appender_flush(appender) when is_reference(appender),
    do: DatabaseThing.NIF.appender_flush(appender)

  @doc """
  Flush the changes made by the appender and close it.

  The appender cannot be used after this point

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _res} = DatabaseThing.query(conn, "CREATE TABLE table_1 (the_n1 INTEGER, the_str1 STRING);")
  iex> {:ok, appender} = DatabaseThing.appender(conn, "table_1")
  iex> :ok = DatabaseThing.appender_add_rows(appender, [[1, "one"], [2, "two"]])
  iex> {:ok, res} = DatabaseThing.query(conn, "SELECT * FROM table_1;")
  iex> [] = DatabaseThing.fetch_all(res)
  iex> :ok = DatabaseThing.appender_close(appender)
  iex> {:ok, res} = DatabaseThing.query(conn, "SELECT * FROM table_1;")
  iex> [[1, "one"], [2, "two"]] = DatabaseThing.fetch_all(res)
  ```
  """
  @spec appender_close(appender()) :: :ok | {:error, reason()}
  def appender_close(appender) when is_reference(appender),
    do: DatabaseThing.NIF.appender_close(appender)

  @doc """
  Returns the version of the linked DuckDB, with a version postfix for dev versions

  Usually used for developing C extensions that must return this for a compatibility check.

  ## Examples
  ```
  iex> DatabaseThing.library_version()
  iex> "v0.7.0"
  ```
  """
  @spec library_version(connection()) :: binary()
  def library_version(connection),
    do: DatabaseThing.NIF.library_version(connection)

  @doc """
  Returns the commit hash of the linked DuckDB library

  ## Examples
  ```
  iex> DatabaseThing.source_id()
  iex> "b00b93f0b1"
  ```
  """
  @spec source_id() :: binary()
  def source_id(),
    do: DatabaseThing.NIF.source_id()

  @doc """
  Returns the Platform of the linked DuckDB library

  ## Examples
  ```
  iex> DatabaseThing.platform()
  iex> "osx_amd64"
  ```
  """
  @spec platform() :: binary()
  def platform(),
    do: DatabaseThing.NIF.platform()

  @doc """
  Returns the count of DuckDB threads

  This is DuckDB own native threads (not a dirty scheduler Erlang threads)
  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> DatabaseThing.number_of_threads(db)
  iex> 8
  ```
  """
  @spec number_of_threads(db()) :: integer()
  def number_of_threads(db),
    do: DatabaseThing.NIF.number_of_threads(db)

  @doc """
  Convert an erlang/elixir integer to a DuckDB hugeint.

  For more information on DuckDB numeric types, see [DuckDB Numeric Data Types](https://duckdb.org/docs/sql/data_types/numeric) For more information on DuckDB numeric types.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _res} = DatabaseThing.query(conn, "CREATE TABLE hugeints(value HUGEINT);")
  iex> {:ok, _res} = DatabaseThing.query(conn, "INSERT INTO hugeints VALUES (98233720368547758080000::hugeint);")
  iex> hugeint = DatabaseThing.integer_to_hugeint(98233720368547758080000)
  iex> {:ok, res} = DatabaseThing.query(conn, "SELECT * FROM hugeints WHERE value = $1", [{:hugeint, hugeint}])
  iex> [[{5325, 4808176044395724800}]] = DatabaseThing.fetch_all(res)
  ```
  """
  def integer_to_hugeint(integer) when is_integer(integer) do
    {:erlang.bsr(integer, 64), :erlang.band(integer, 0xFFFFFFFFFFFFFFFF)}
  end

  @doc """
  Convert a duckdb hugeint record to erlang/elixir integer.

  ## Examples
  ```
  iex> {:ok, db} = DatabaseThing.open()
  iex> {:ok, conn} = DatabaseThing.connection(db)
  iex> {:ok, _res} = DatabaseThing.query(conn, "CREATE TABLE hugeints(value HUGEINT);")
  iex> {:ok, _res} = DatabaseThing.query(conn, "INSERT INTO hugeints VALUES (98233720368547758080000::hugeint);")
  iex> {:ok, res} = DatabaseThing.query(conn, "SELECT * FROM hugeints;")
  iex> [[hugeint = {5325, 4808176044395724800}]] = DatabaseThing.fetch_all(res)
  iex> 98233720368547758080000 = DatabaseThing.hugeint_to_integer(hugeint)
  ```
  """
  def hugeint_to_integer({upper, lower}) when is_integer(upper) and is_integer(lower) and lower >= 0 do
    upper |> :erlang.bsl(64) |> :erlang.bor(lower)
  end
end
