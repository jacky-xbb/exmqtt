defmodule Exmqtt.Ws do

  @ws_headers [{"cache-control", "no-cache"}]
  @ws_opts %{compress: false, protocols: [{<<"mqtt">>, :gun_ws_h}]}

  @type option() :: {:ws_path, charlist()}

  def connect(host, port, opts, timeout) do
    {:ok, _} = :application.ensure_all_started(:gun)
    # 1. open connection
    conn_opts = %{
      connect_timeout: timeout,
      retry: 3,
      retry_timeout: 30000
    }
    case :gun.open(host, port, conn_opts) do
        {:ok, conn_pid} ->
            {:ok, _} = :gun.await_up(conn_pid, timeout)
            case upgrade(conn_pid, opts, timeout) do
                {:ok, _headers} -> {:ok, conn_pid}
                error -> error
            end
        error -> error
    end
  end

  @spec upgrade(pid(), list(), timeout()) :: {:ok, headers :: list()} | {:error, reason :: term()}
  def upgrade(conn_pid, opts, timeout) do
    # 2. websocket upgrade
    path = Keyword.get(opts, :ws_path, "/mqtt")
    stream_ref = :gun.ws_upgrade(conn_pid, path, @ws_headers, @ws_opts)
    receive do
        {:gun_upgrade, ^conn_pid, ^stream_ref, [<<"websocket">>], headers} ->
          {:ok, headers}
        {:gun_response, ^conn_pid, _, _, status, headers} ->
          {:error, {:ws_upgrade_failed, status, headers}};
        {:gun_error, ^conn_pid, ^stream_ref, reason} ->
          {:error, {:ws_upgrade_failed, reason}}
    after timeout ->
        {:error, :timeout}
    end
  end

  # fake stats:)
  def getstat(_ws_pid, options) do
    {:ok, Enum.map(options, fn opt -> {opt, 0} end)}
  end

  def setopts(_ws_pid, _opts) do
    :ok
  end

  @spec send(pid(), iodata()) :: :ok
  def send(ws_pid, data) do
    :gun.ws_send(ws_pid, {:binary, data})
  end

  @spec close(pid()) :: :ok
  def close(ws_pid) do
    :gun.shutdown(ws_pid)
  end
end
