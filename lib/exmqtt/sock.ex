defmodule Exmqtt.Sock do
  require Record

  Record.defrecord(:ssl_socket, [:tcp, :ssl])

  @type ssl_socket :: record(:ssl_socket, tcp: :erlang.socket(), ssl: :erlang.sslsocket())

  @type socket() :: :init.socket() | ssl_socket()
  @type sockname() :: {:inet.ip_address(), :inet.port_number()}
  @type option() :: :gen_tcp.connect_option() | {:ssl_opts, [:ssl.ssl_option()]}

  @default_tcp_options [:binary, {:packet, :raw}, {:active, false},
                        {:nodelay, true}, {:reuseaddr, true}]

  @spec connect(:inet.ip_address() | :inet.hostname(), :inet.port_number(), [option()], timeout())
                :: {:ok, socket()} | {:error, term()}
  def connect(host, port, sock_opts, timeout) do
    tcp_opts = merge_opts(@default_tcp_options, List.keydelete(sock_opts, :ssl_opts, 0))
    case :gen_tcp.connect(host, port, tcp_opts, timeout) do
      {:ok, sock} ->
        case List.keyfind(sock_opts, :ssl_opts, 0) do
          {:ssl_opts, ssl_opts} ->
            ssl_upgrade(sock, ssl_opts, timeout)
          false ->
            {:ok, sock}
        end
      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec send_data(:erlang.socket(), iodata()) :: :ok | {:error, :einval | :closed}
  def send_data(sock, data) when is_port(sock) do
    try do
      :erlang.port_command(sock, data)
      :ok
    rescue
      ArgumentError -> {:error, :einval}
    end
  end
  def send_data(ssl_socket(ssl: ssl_sock), data) do
    :ssl.send(ssl_sock, data)
  end

  @spec close(socket()) :: :ok | {:error, any}
  def close(sock) when is_port(sock), do: :gen_tcp.close(sock)
  def close(ssl_socket(ssl: ssl_sock)), do: :ssl.close(ssl_sock)

  @spec setopts(socket(), [:gen_tcp.option() | :ssl.socketoption()]) :: :ok | {:error, any}
  def setopts(sock, opts) when is_port(sock), do: :inet.setopts(sock, opts)
  def setopts(ssl_socket(ssl: ssl_sock), opts), do: :ssl.setopts(ssl_sock, opts)

  @spec getstat(socket(), [atom()]) :: {:ok, [{atom(), integer()}]} | {:error, term()}
  def getstat(sock, options) when is_port(sock), do: :inet.getstat(sock, options)
  def getstat(ssl_socket(tcp: sock), options), do: :inet.getstat(sock, options)

  @spec sockname(socket()) :: {:ok, sockname()} | {:error, term()}
  def sockname(sock) when is_port(sock), do: :inet.sockname(sock)
  def sockname(ssl_socket(ssl: ssl_sock)), do: :ssl.sockname(ssl_sock)

  def ssl_upgrade(sock, ssl_opts, timeout) do
    tls_versions = Keyword.get(ssl_opts, :versions, [])
    ciphers = Keyword.get(ssl_opts, :ciphers, default_ciphers(tls_versions))
    ssl_opts2 = merge_opts(ssl_opts, [{:ciphers, ciphers}])
    case :ssl.connect(sock, ssl_opts2, timeout) do
      {:ok, ssl_sock} ->
        :ok = :ssl.controlling_process(ssl_sock, self())
        {:ok, ssl_socket(tcp: sock, ssl: ssl_sock)}
      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec merge_opts(list(), list()) :: list()
  def merge_opts(defaults, options) do
    List.foldl(options, defaults, fn
      {opt, val}, acc ->
        List.keystore(acc, opt, 0, {opt, val})
      opt, acc ->
        [opt | acc] |> Enum.uniq() |> Enum.sort()
      end)
  end

  def default_ciphers(tls_versions) do
    List.foldl(tls_versions, [], fn tls_ver, ciphers ->
      ciphers ++ :ssl.cipher_suites(:all, tls_ver)
    end)
  end
end
