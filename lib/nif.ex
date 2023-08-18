defmodule DatabaseThing.NIF do
  @moduledoc false

  use Rustler,
  otp_app: :database_thing,
  crate: :duckling

  @type db() :: reference()
  @type connection() :: reference()
  @type query_result() :: reference()
  @type statement() :: reference()
  @type appender :: reference()
  @type reason() :: :atom | binary()
  @type config() :: DatabaseThing.Config.t | nil

  @spec open(binary(), config()) :: {:ok, db()} | {:error, reason()}
  def open(_path, _config), do: :erlang.nif_error(:not_loaded)

  @spec close(db()) :: :ok | {:error, reason()}
  def close(_database), do: :erlang.nif_error(:not_loaded)

  @spec connection(db()) :: {:ok, connection()} | {:error, reason()}
  def connection(_database), do: :erlang.nif_error(:not_loaded)

  @spec query(connection(), binary()) :: {:ok, query_result()} | {:error, reason()}
  def query(_connection, _string_sql, []), do: :erlang.nif_error(:not_loaded)

  @spec query(connection(), binary(), list()) :: {:ok, query_result()} | {:error, reason()}
  def query(_connection, _string_sql, _args), do: :erlang.nif_error(:not_loaded)

  @spec prepare_statement(connection(), binary()) :: {:ok, statement()} | {:error, reason()}
  def prepare_statement(_connection, _string_sql), do: :erlang.nif_error(:not_loaded)

  @spec execute_statement(statement()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(_statement, []), do: :erlang.nif_error(:not_loaded)

  @spec execute_statement(statement(), list()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(_statement, _args), do: :erlang.nif_error(:not_loaded)

  @spec get_column_names(query_result()) :: list()
  def get_column_names(_query_result), do: :erlang.nif_error(:not_loaded)

  @spec fetch_chunk(query_result()) :: list()
  def fetch_chunk(_query_result), do: :erlang.nif_error(:not_loaded)

  @spec fetch_all(query_result()) :: list()
  def fetch_all(_query_result), do: :erlang.nif_error(:not_loaded)

  @spec appender(connection(), binary()) :: {:ok, appender()} | {:error, reason()}
  def appender(_connection, _table_name), do: :erlang.nif_error(:not_loaded)

  @spec appender_add_row(appender(), list()) :: :ok | {:error, reason()}
  def appender_add_row(_appender, _row), do: :erlang.nif_error(:not_loaded)

  @spec appender_add_rows(appender(), list(list())) :: :ok | {:error, reason()}
  def appender_add_rows(_appender, _rows), do: :erlang.nif_error(:not_loaded)

  @spec appender_flush(appender()) :: :ok | {:error, reason()}
  def appender_flush(_appender), do: :erlang.nif_error(:not_loaded)

  @spec appender_close(appender()) :: :ok | {:error, reason()}
  def appender_close(_appender), do: :erlang.nif_error(:not_loaded)

  @spec library_version(connection()) :: binary()
  def library_version(_connection), do: :erlang.nif_error(:not_loaded)

  @spec source_id() :: binary()
  def source_id(), do: :erlang.nif_error(:not_loaded)

  @spec platform() :: binary()
  def platform(), do: :erlang.nif_error(:not_loaded)

  @spec number_of_threads(db()) :: integer()
  def number_of_threads(_database), do: :erlang.nif_error(:not_loaded)

  @spec extension_is_loaded(db(), binary()) :: boolean()
  def extension_is_loaded(_database, _extension_name), do: :erlang.nif_error(:not_loaded)
end
