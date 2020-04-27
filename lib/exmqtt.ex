defmodule QoS do
  require ExmqttConstants
  alias ExmqttConstants, as: Const
  @spec is_qos(any) :: boolean
  defmacro is_qos(i) do
    quote do
      unquote(i >= Const.qos_0 and i <= Const.qos_2)
    end
  end
end

defmodule Exmqtt do
  use GenStateMachine, callback_mode: :state_functions

  require Record
  require Logger
  require ExmqttConstants
  require QoS

  alias ExmqttConstants, as: Const
  alias Exmqtt.Packet
  alias Exmqtt.Frame
  alias Exmqtt.Props
  alias Exmqtt.Sock
  alias Exmqtt.Ws
  alias Exmqtt.Errors

  @type host() :: :inet.ip_address() | :inet.hostname()
  @type maybe(t) :: nil | t
  @type topic() :: binary()
  @type payload() :: iodata()
  @type packet_id() :: 0..0xFF
  @type reason_code() :: 0..0xFF
  @type properties() :: %{String.t() => term()}
  @type version() :: Const.mqtt_proto_v3 | Const.mqtt_proto_v4 | Const.mqtt_proto_v5
  @type qos() :: Const.qos_0 | Const.qos_1 | Const.qos_2
  @type qos_name() :: :qos0 | :at_most_once
                      |:qos1 | :at_least_once
                      |:qos2 | :exactly_once
  @type pubopt() :: {:retain, boolean()}
                    | {:qos, qos() | qos_name()}
                    | {:timeout, timeout()}
  @type subopt() :: {:rh, 0 | 1 | 2}
                    | {:rap, boolean()}
                    | {:nl,  boolean()}
                    | {:qos, qos() | qos_name()}

  @type subscribe_ret() :: {:ok, properties(), [reason_code()]} | {:error, term()}
  @type conn_mod() :: Sock | Ws
  @type client() :: pid() | atom()
  @opaque mqtt_msg() :: %Packet.Msg{}

  # Message handler is a set of callbacks defined to handle MQTT messages
  # as well as the disconnect event.
  @type msg_handler() :: %{
    puback: (() -> any()),
    publish: ((:emqx_types.message()) -> any()),
    disconnected: (({reason_code(), _properties :: term()}) -> any())
  }

  @type option() :: {:name, atom()}
                  | {:owner, pid()}
                  | {:msg_handler, msg_handler()}
                  | {:host, host()}
                  | {:hosts, [{host(), :inet.port_number()}]}
                  | {:port, :inet.port_number()}
                  | {:tcp_opts, [:gen_tcp.option()]}
                  | {:ssl, boolean()}
                  | {:ssl_opts, [:ssl.ssl_option()]}
                  | {:ws_path, charlist()}
                  | {:connect_timeout, pos_integer()}
                  | {:bridge_mode, boolean()}
                  | {:clientid, iodata()}
                  | {:clean_start, boolean()}
                  | {:username, iodata()}
                  | {:password, iodata()}
                  | {:proto_ver, :v3 | :v4 | :v5}
                  | {:keepalive, non_neg_integer()}
                  | {:max_inflight, pos_integer()}
                  | {:retry_interval, timeout()}
                  | {:will_topic, iodata()}
                  | {:will_payload, iodata()}
                  | {:will_retain, boolean()}
                  | {:will_qos, qos()}
                  | {:will_props, properties()}
                  | {:auto_ack, boolean()}
                  | {:ack_timeout, pos_integer()}
                  | {:force_ping, boolean()}
                  | {:properties, properties()}

  Record.defrecord(:state, [
    :name, :owner, :msg_handler, :host, :port, :hosts, :conn_mod, :socket,
    :sock_opts, :connect_timeout, :bridge_mode, :clientid, :clean_start,
    :username, :password, :proto_ver, :proto_name, :keepalive, :keepalive_timer,
    :force_ping, :paused, :will_flag, :will_msg, :properties, :pending_calls,
    :subscriptions, :max_inflight, :inflight, :awaiting_rel, :auto_ack, :ack_timeout,
    :ack_timer, :retry_interval, :retry_timer, :session_present, :last_packet_id, :parse_state
  ])

  @type state :: record(:state,
    name:            atom(),
    owner:           pid(),
    msg_handler:     Const.no_msg_hdlr | msg_handler(),
    host:            host(),
    port:            :inet.port_number(),
    hosts:           [{host(), :inet.port_number()}],
    conn_mod:        conn_mod(),
    socket:          :inet.socket() | pid(),
    sock_opts:       [Sock.option() | Ws.option()],
    connect_timeout: pos_integer(),
    bridge_mode:     boolean(),
    clientid:        binary(),
    clean_start:     boolean(),
    username:        maybe(binary()),
    password:        maybe(binary()),
    proto_ver:       version(),
    proto_name:      iodata(),
    keepalive:       non_neg_integer(),
    keepalive_timer: maybe(reference()),
    force_ping:      boolean(),
    paused:          boolean(),
    will_flag:       boolean(),
    will_msg:        mqtt_msg(),
    properties:      properties(),
    pending_calls:   list(),
    subscriptions:   map(),
    max_inflight:    :infinity | pos_integer(),
    inflight:        %{packet_id() => term()},
    awaiting_rel:    map(),
    auto_ack:        boolean(),
    ack_timeout:     pos_integer(),
    ack_timer:       reference(),
    retry_interval:  pos_integer(),
    retry_timer:     reference(),
    session_present: boolean(),
    last_packet_id:  packet_id(),
    parse_state:     Frame.parse_state()
  )

  Record.defrecord(:call, [:id, :from, :req, :ts])

  # -----------------------------------------------------------------
  # API
  # -----------------------------------------------------------------
  @spec start_link() :: :gen_statem.start_ret()
  def start_link(), do: start_link([])

  @spec start_link(map() | [option()]) :: :gen_statem.start_ret()
  def start_link(options) when is_map(options) do
    start_link(Map.to_list(options))
  end

  def start_link(options) when is_list(options) do
    :ok = Props.validate(Keyword.get(options, :properties, %{}))
    case Keyword.get(options, :name) do
      nil ->
        GenStateMachine.start_link(__MODULE__, [with_owner(options)], [])
      reg_name when is_atom(reg_name) ->
        GenStateMachine.start_link(__MODULE__, [with_owner(options)], name: reg_name)
    end
  end

  @spec connect(client()) :: {:ok, properties()} | {:error, term()}
  def connect(client) do
    do_call(client, {:connect, Sock})
  end

  def ws_connect(client) do
    do_call(client, {:connect, Ws})
  end

  # @private
  def do_call(client, req) do
    GenStateMachine.call(client, req, :infinity)
  end

  @spec subscribe(client(), topic() | {topic(), qos() | qos_name() | [subopt()]} | [{topic(), qos()}])
        :: subscribe_ret()
  def subscribe(client, topic) when is_binary(topic) do
    subscribe(client, {topic, Const.qos_0})
  end
  def subscribe(client, {topic, qos}) when is_binary(topic) and is_atom(qos) do
    subscribe(client, [{topic, qos_i(qos)}])
  end

  def subscribe(client, {topic, qos})
    when is_binary(topic) and qos >= Const.qos_0 and qos <= Const.qos_2 do
    subscribe(client, [{topic, qos_i(qos)}])
  end
  def subscribe(client, topics) when is_list(topics) do
    subscribe(client,
              %{},
              Enum.map(
              topics,
              fn
                {topic, qos} when is_binary(topic) and is_atom(qos) -> {topic, [{:qos, qos_i(qos)}]}
                {topic, qos} when is_binary(topic) and qos >= Const.qos_0 and qos <= Const.qos_2 -> {topic, [{:qos, qos_i(qos)}]}
                {topic, opts} when is_binary(topic) and is_list(opts) -> {topic, opts}
              end))
  end

  @spec subscribe(client(), topic(), qos() | qos_name() | [subopt()]) :: subscribe_ret()
  def subscribe(client, topic, qos) when is_binary(topic) and is_atom(qos) do
    subscribe(client, topic, qos_i(qos))
  end
  def subscribe(client, topic, qos)
    when is_binary(topic) and qos >= Const.qos_0 and qos <= Const.qos_2 do
    subscribe(client, topic, [{:qos, qos}])
  end
  def subscribe(client, topic, opts) when is_binary(topic) and is_list(opts) do
    subscribe(client, %{}, [{topic, opts}])
  end
  @spec subscribe(client(), properties(), [{topic(), qos() | [subopt()]}]) :: subscribe_ret()
  def subscribe(client, properties, topics) when is_map(properties) and is_list(topics) do
    topics1 = for {topic, opts} <- topics do
      {topic, parse_subopt(opts)}
    end
    GenStateMachine.call(client, {:subscribe, properties, topics1})
  end

  @spec subscribe(client(), properties(), topic(), qos() | qos_name() | [subopt()]) :: subscribe_ret()
  def subscribe(client, properties, topic, qos)
      when is_map(properties) and is_binary(topic) and is_atom(qos) do
    subscribe(client, properties, topic, qos_i(qos))
  end
  def subscribe(client, properties, topic, qos)
      when is_map(properties) and is_binary(topic) and qos >= Const.qos_0 and qos <= Const.qos_2 do
    subscribe(client, properties, topic, [{:qos, qos}])
  end
  def subscribe(client, properties, topic, opts)
      when is_map(properties) and is_binary(topic) and is_list(opts) do
    subscribe(client, properties, [{topic, opts}])
  end

  def parse_subopt(opts) do
    parse_subopt(opts, %{rh: 0, rap: 0, nl: 0, qos: Const.qos_0})
  end

  def parse_subopt([], result) do
    result
  end
  def parse_subopt([{:rh, i} | opts], result) when i >= 0 and i <= 2 do
    parse_subopt(opts, %{result | rh: i})
  end
  def parse_subopt([{:rap, true} | opts], result) do
    parse_subopt(opts, %{result | rap: 1})
  end
  def parse_subopt([{:rap, false} | opts], result) do
    parse_subopt(opts, %{result | rap: 0})
  end
  def parse_subopt([{:nl, true} | opts], result) do
    parse_subopt(opts, %{result | nl: 1})
  end
  def parse_subopt([{:nl, false} | opts], result) do
    parse_subopt(opts, %{result | nl: 0})
  end
  def parse_subopt([{:qos, qos} | opts], result) do
    parse_subopt(opts, %{result | qos: qos_i(qos)})
  end

  @spec publish(client(), topic(), payload()) :: :ok | {:error, term()}
  def publish(client, topic, payload) when is_binary(topic) do
    publish(client, %Packet.Msg{topic: topic, qos: Const.qos_0, payload: :erlang.iolist_to_binary(payload)})
  end

  @spec publish(client(), topic(), payload(), qos() | qos_name() | [pubopt()])
        :: :ok | {:ok, packet_id()} | {:error, term()}
  def publish(client, topic, payload, qos) when is_binary(topic) and is_atom(qos) do
    publish(client, topic, payload, [{:qos, qos_i(qos)}])
  end
  def publish(client, topic, payload, qos)
    when is_binary(topic) and qos >= Const.qos_0 and qos <= Const.qos_2 do
    publish(client, topic, payload, [{:qos, qos}])
  end
  def publish(client, topic, payload, opts) when is_binary(topic) and is_list(opts) do
    publish(client, topic, %{}, payload, opts)
  end

  @spec publish(client(), topic(), properties(), payload(), [pubopt()])
        :: :ok | {:ok, packet_id()} | {:error, term()}
  def publish(client, topic, properties, payload, opts)
      when is_binary(topic) and is_map(properties) and is_list(opts) do
    :ok = Props.validate(properties)
    retain = Keyword.get(opts, :retain)
    qos = qos_i(Keyword.get(opts, :qos, Const.qos_0))
    publish(client, %Packet.Msg{qos: qos,
                                retain: retain,
                                topic: topic,
                                props: properties,
                                payload: :erlang.iolist_to_binary(payload)})

  end

  @spec publish(client(), %Packet.Msg{}) :: :ok | {:ok, packet_id()} | {:error, term()}
  def publish(client, msg) do
    GenStateMachine.call(client, {:publish, msg})
  end

  @spec unsubscribe(client(), topic() | [topic()]) :: subscribe_ret()
  def unsubscribe(client, topic) when is_binary(topic) do
    unsubscribe(client, [topic])
  end
  def unsubscribe(client, topics) when is_list(topics) do
    unsubscribe(client, %{}, topics)
  end

  @spec unsubscribe(client(), properties(), topic() | [topic()]) :: subscribe_ret()
  def unsubscribe(client, properties, topic) when is_map(properties) and is_binary(topic) do
    unsubscribe(client, properties, [topic])
  end
  def unsubscribe(client, properties, topics) when is_map(properties) and is_list(topics) do
    GenStateMachine.call(client, {:unsubscribe, properties, topics})
  end

  @spec ping(client()) :: :pong
  def ping(client) do
    GenStateMachine.call(client, :ping)
  end

  @spec disconnect(client()) :: :ok
  def disconnect(client) do
    disconnect(client, Const.rc_success)
  end

  @spec disconnect(client(), reason_code()) :: :ok
  def disconnect(client, reason_code) do
    disconnect(client, reason_code, %{})
  end

  @spec disconnect(client(), reason_code(), properties()) :: :ok
  def disconnect(client, reason_code, properties) do
    GenStateMachine.call(client, {:disconnect, reason_code, properties})
  end

  # --------------------------------------------------------------------
  # For test cases
  # --------------------------------------------------------------------
  def puback(client, packet_id) when is_integer(packet_id) do
    puback(client, packet_id, Const.rc_success)
  end
  def puback(client, packet_id, reason_code)
      when is_integer(packet_id) and is_integer(reason_code) do
    puback(client, packet_id, reason_code, %{})
  end
  def puback(client, packet_id, reason_code, properties)
      when is_integer(packet_id) and is_integer(reason_code) and is_map(properties) do
    GenStateMachine.cast(client, {:puback, packet_id, reason_code, properties})
  end

  def pubrec(client, packet_id) when is_integer(packet_id) do
    pubrec(client, packet_id, Const.rc_success)
  end
  def pubrec(client, packet_id, reason_code)
      when is_integer(packet_id) and is_integer(reason_code) do
    pubrec(client, packet_id, reason_code, %{})
  end
  def pubrec(client, packet_id, reason_code, properties)
      when is_integer(packet_id) and is_integer(reason_code) and is_map(properties) do
    GenStateMachine.cast(client, {:pubrec, packet_id, reason_code, properties})
  end

  def pubrel(client, packet_id) when is_integer(packet_id) do
    pubrel(client, packet_id, Const.rc_success)
  end
  def pubrel(client, packet_id, reason_code)
      when is_integer(packet_id) and is_integer(reason_code) do
    pubrel(client, packet_id, reason_code, %{})
  end
  def pubrel(client, packet_id, reason_code, properties)
      when is_integer(packet_id) and is_integer(reason_code) and is_map(properties) do
    GenStateMachine.cast(client, {:pubrel, packet_id, reason_code, properties})
  end

  def pubcomp(client, packet_id) when is_integer(packet_id) do
    pubcomp(client, packet_id, Const.rc_success)
  end
  def pubcomp(client, packet_id, reason_code)
      when is_integer(packet_id) and is_integer(reason_code) do
    pubcomp(client, packet_id, reason_code, %{})
  end
  def pubcomp(client, packet_id, reason_code, properties)
      when is_integer(packet_id) and is_integer(reason_code) and is_map(properties) do
    GenStateMachine.cast(client, {:pubcomp, packet_id, reason_code, properties})
  end

  def subscriptions(client) do
    GenStateMachine.call(client, :subscriptions)
  end

  def info(client) do
    GenStateMachine.call(client, :info)
  end

  def stop(client) do
    GenStateMachine.call(client, :stop)
  end

  def pause(client) do
    GenStateMachine.call(client, :pause)
  end

  def resume(client) do
    GenStateMachine.call(client, :resume)
  end

  # --------------------------------------------------------------------
  # gen_statem callbacks
  # --------------------------------------------------------------------
  def init([options]) do
    Process.flag(:trap_exit, true)
    client_id = case {Keyword.get(options, :proto_ver, :v4),
                      Keyword.get(options, :clientid)} do
      {:v5, nil} -> Const.no_client_id
      {_ver, nil} -> random_client_id()
      {_ver, id} -> :erlang.iolist_to_binary(id)
    end

    # Set client ID for logger metadata
    Logger.metadata(client_id: client_id)

    state = init(options, state(
      host:             {127,0,0,1},
      port:             1883,
      hosts:            [],
      conn_mod:         Sock,
      sock_opts:        [],
      bridge_mode:      false,
      clientid:         client_id,
      clean_start:      true,
      proto_ver:        Const.mqtt_proto_v4,
      proto_name:       <<"MQTT">>,
      keepalive:        Const.default_keepalive,
      force_ping:       false,
      paused:           false,
      will_flag:        false,
      will_msg:         %Packet.Msg{},
      pending_calls:    [],
      subscriptions:    %{},
      max_inflight:     :infinity,
      inflight:         %{},
      awaiting_rel:     %{},
      properties:       %{},
      auto_ack:         true,
      ack_timeout:      Const.default_ack_timeout,
      retry_interval:   0,
      connect_timeout:  Const.default_connect_timeout,
      last_packet_id:   1
    ))
    {:ok, :initialized, init_parse_state(state)}
  end

  def init([], state) do
    state
  end
  def init([{:name, name} | opts], state) do
    init(opts, state(state, name: name))
  end
  def init([{:owner, owner} | opts], state) when is_pid(owner) do
    Process.link(owner)
    init(opts, state(state, owner: owner))
  end
  def init([{:msg_handler, hdlr} | opts], state) do
    init(opts, state(state, msg_handler: hdlr))
  end
  def init([{:host, host} | opts], state) do
    init(opts, state(state, host: host))
  end
  def init([{:port, port} | opts], state) do
    init(opts, state(state, port: port))
  end
  def init([{:hosts, hosts} | opts], state) do
    hosts1 = hosts |> List.foldl([], fn
      {host, port}, acc -> [{host, port} | acc]
      host, acc -> [{host, 1883} | acc]
    end)
    init(opts, state(state, hosts: hosts1))
  end
  def init([{:tcp_opts, tcp_opts} | opts], state(sock_opts: sock_opts) = state) do
    init(opts, state(state, sock_opts: merge_opts(sock_opts, tcp_opts)))
  end
  def init([{:ssl, enable_ssl} | opts], state) do
      case List.keytake(opts, :ssl_opts, 0) do
        {ssl_opts, without_ssl_opts} ->
          init([ssl_opts, {:ssl, enable_ssl} | without_ssl_opts], state)
        nil ->
          init([{:ssl_opts, []}, {:ssl, enable_ssl}| opts], state)
      end
  end
  def init([{:ssl_opts, ssl_opts} | opts], state(sock_opts: sock_opts) = state) do
     case List.keytake(opts, :ssl, 0) do
      {{:ssl, true}, without_enable_ssl} ->
        :ok = :ssl.start()
        ssl_opts1 = merge_opts(sock_opts, [{ssl_opts, ssl_opts}])
        init(without_enable_ssl, state(state, sock_opts: ssl_opts1))
      {{:ssl, false}, without_enable_ssl} ->
        init(without_enable_ssl, state)
      nil ->
        init(opts, state)
    end
  end
  def init([{:ws_path, path} | opts], state(sock_opts: sock_opts) = state) do
    init(opts, state(state, sock_opts: [{:ws_path, path} | sock_opts]))
  end
  def init([{:clientid, client_id} | opts], state) do
    init(opts, state(state, clientid: :erlang.iolist_to_binary(client_id)))
  end
  def init([{:clean_start, clean_start} | opts], state) when is_boolean(clean_start) do
    init(opts, state(state, clean_start: clean_start))
  end
  def init([{:username, username} | opts], state) do
    init(opts, state(state, username: :erlang.iolist_to_binary(username)))
  end
  def init([{:password, password} | opts], state) do
    init(opts, state(state, password: :erlang.iolist_to_binary(password)))
  end
  def init([{:keepalive, secs} | opts], state) do
    init(opts, state(state, keepalive: secs))
  end
  def init([{:proto_ver, :v3} | opts], state) do
    init(opts, state(state, proto_ver: Const.mqtt_proto_v3, proto_name: <<"MQIsdp">>))
  end
  def init([{:proto_ver, :v4} | opts], state) do
    init(opts, state(state, proto_ver: Const.mqtt_proto_v4, proto_name: <<"MQTT">>))
  end
  def init([{:proto_ver, :v5} | opts], state) do
    init(opts, state(state, proto_ver: Const.mqtt_proto_v5, proto_name: <<"MQTT">>))
  end
  def init([{:will_topic, topic} | opts], state(will_msg: will_msg) = state) do
    will_msg1 = init_will_msg({:topic, topic}, will_msg)
    init(opts, state(state, will_flag: true, will_msg: will_msg1))
  end
  def init([{:will_props, properties} | opts], state(will_msg: will_msg) = state) do
    init(opts, state(state, will_msg: init_will_msg({:props, properties}, will_msg)))
  end
  def init([{:will_payload, payload} | opts], state(will_msg: will_msg) = state) do
    init(opts, state(state, will_msg: init_will_msg({:payload, payload}, will_msg)))
  end
  def init([{:will_retain, retain} | opts], state(will_msg: will_msg) = state) do
    init(opts, state(state, will_msg: init_will_msg({:retain, retain}, will_msg)))
  end
  def init([{:will_qos, qos} | opts], state(will_msg: will_msg) = state) do
    init(opts, state(state, will_msg: init_will_msg({:qos, qos}, will_msg)))
  end
  def init([{:connect_timeout, timeout}| opts], state) do
    init(opts, state(state, connect_timeout: :timer.seconds(timeout)))
  end
  def init([{:ack_timeout, timeout}| opts], state) do
    init(opts, state(state, ack_timeout: :timer.seconds(timeout)))
  end
  def init([:force_ping | opts], state) do
    init(opts, state(state, force_ping: true))
  end
  def init([{:force_ping, force_ping} | opts], state) when is_boolean(force_ping) do
    init(opts, state(state, force_ping: force_ping))
  end
  def init([{:properties, properties} | opts], state(properties: init_props) = state) do
    init(opts, state(state, properties: Map.merge(init_props, properties)))
  end
  def init([{:max_inflight, :infinity} | opts], state) do
    init(opts, state(state, max_inflight: :infinity, inflight: %{}))
  end
  def init([{:max_inflight, i} | opts], state) when is_integer(i) do
    init(opts, state(state, max_inflight: i, inflight: %{}))
  end
  def init([:auto_ack | opts], state) do
    init(opts, state(state, auto_ack: true))
  end
  def init([{:auto_ack, auto_ack} | opts], state) when is_boolean(auto_ack) do
    init(opts, state(state, auto_ack: auto_ack))
  end
  def init([{:retry_interval, i} | opts], state) do
    init(opts, state(state, retry_interval: :timer.seconds(i)))
  end
  def init([{:bridge_mode, mode} | opts], state) when is_boolean(mode) do
    init(opts, state(state, bridge_mode: mode))
  end
  def init([_opt | opts], state) do
      init(opts, state)
  end

  def init_will_msg({:topic, topic}, %Packet.Msg{} = will_msg) do
    %{will_msg | topic: :erlang.iolist_to_binary(topic)}
  end
  def init_will_msg({:props, props}, %Packet.Msg{} = will_msg) do
    %{will_msg | props: props}
  end
  def init_will_msg({:payload, payload}, %Packet.Msg{} = will_msg) do
    %{will_msg | payload: :erlang.iolist_to_binary(payload)}
  end
  def init_will_msg({:retain, retain}, %Packet.Msg{} = will_msg) when is_boolean(retain) do
    %{will_msg | retain: retain}
  end
  def init_will_msg({:qos, qos}, %Packet.Msg{} = will_msg) do
    %{will_msg | qos: qos_i(qos)}
  end

  def init_parse_state(state(proto_ver: ver, properties: properties) = state) do
    max_size = Map.get(properties, "Maximum-Packet-Size", Const.max_packet_size)
    parse_state = Frame.initial_parse_state(%{max_size: max_size, version: ver})
    state(state, parse_state: parse_state)
  end

  # def callback_mode() do
  #   :state_functions
  # end

  def initialized({:call, from}, {:connect, conn_mod}, state(sock_opts: sock_opts,
    connect_timeout: timeout) = state) do
    case sock_connect(conn_mod, hosts(state), sock_opts, timeout) do
        {:ok, sock} ->
          case mqtt_connect(run_sock(state(state, conn_mod: conn_mod, socket: sock))) do
            {:ok, new_state} ->
              {:next_state, :waiting_for_connack, add_call(new_call(:connect, from), new_state), [timeout]}
            {:error, reason} = error ->
              {:stop_and_reply, reason, [{:reply, from, error}]}
          end
        {:error, reason} = error ->
          {:stop_and_reply, {:shutdown, reason}, [{:reply, from, error}]}
    end
  end

  def initialized(event_type, event_content, state) do
    handle_event(event_type, event_content, :initialized, state)
  end

  def mqtt_connect(state(clientid: client_id,
                         clean_start: clean_start,
                         bridge_mode: is_bridge,
                         username: username,
                         password: password,
                         proto_ver: proto_ver,
                         proto_name: proto_name,
                         keepalive: keepalive,
                         will_flag: will_flag,
                         will_msg: will_msg,
                         properties: properties) = state) do
  %Packet.Msg{
    qos: will_qos,
    retain: will_retain,
    topic: will_topic,
    props: will_props,
    payload: will_payload} = will_msg
  conn_props = Props.filter(Const.connect, properties)
  send_data(Packet.connect_packet(%Packet.Connect{
                                  proto_ver: proto_ver,
                                  proto_name: proto_name,
                                  is_bridge: is_bridge,
                                  clean_start: clean_start,
                                  will_flag: will_flag,
                                  will_qos: will_qos,
                                  will_retain: will_retain,
                                  keepalive: keepalive,
                                  properties: conn_props,
                                  clientid: client_id,
                                  will_props: will_props,
                                  will_topic: will_topic,
                                  will_payload: will_payload,
                                  username: username,
                                  password: password}), state)
  end

  def waiting_for_connack(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.connack()},
      variable: %Packet.Connack{
        ack_flags: sess_present,
        reason_code: Const.rc_success,
        properties: properties
      }
    },
    state(properties: all_props, clientid: client_id) = state) do
    case take_call(:connect, state) do
      {:value, call(from: from), state1} ->
        all_props1 = case properties do
                      nil -> all_props
                      _ -> Map.merge(all_props, properties)
                    end
        reply = {:ok, properties}
        state2 = state(state1,
                       clientid: assign_id(client_id, all_props1),
                       properties: all_props1,
                       session_present: sess_present)

        {:next_state, :connected, ensure_keepalive_timer(state2), [{:reply, from, reply}]}
      false ->
        {:stop, :bad_connack}
    end
  end

  def waiting_for_connack(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.connack()},
      variable: %Packet.Connack{
        ack_flags: _sess_present,
        reason_code: reason_code,
        properties: properties
      }
    },
    state(proto_ver: proto_ver) = state) do
    reason = reason_code_name(reason_code, proto_ver)
    case take_call(:connect, state) do
      {:value, call(from: from), _state} ->
        reply = {:error, {reason, properties}}
        {:stop_and_reply, {:shutdown, Reason}, [{:reply, from, reply}]}
      false -> {:stop, :connack_error}
    end
  end

  def waiting_for_connack(:timeout, _timeout, state) do
    case take_call(:connect, state) do
      {:value, call(from: from), _state} ->
          reply = {:error, :connack_timeout}
          {:stop_and_reply, :connack_timeout, [{:reply, from, reply}]}
      false -> {:stop, :connack_timeout}
    end
  end

  def waiting_for_connack(event_type, event_content, state) do
    case take_call(:connect, state) do
      {:value, call(from: from), _state} ->
        case handle_event(event_type, event_content, :waiting_for_connack, state) do
            {:stop, reason, _state} ->
                reply = {:error, {reason, event_content}}
                {:stop_and_reply, reason, [{:reply, from, reply}]}
            state_callback_result ->
              state_callback_result
        end
      false -> {:stop, :connack_timeout}
    end
  end

  def connected({:call, from}, :subscriptions, state(subscriptions: subscriptions)) do
    {:keep_state_and_data, [{:reply, from, Map.to_list(subscriptions)}]}
  end

  def connected({:call, from}, :info, state) do
    info = Enum.zip(record_info(state), tl(Tuple.to_list(state)))
    {:keep_state_and_data, [{:reply, from, info}]}
  end

  def connected({:call, from}, :pause, state) do
    {:keep_state, state(state, paused: true), [{:reply, from, :ok}]}
  end

  def connected({:call, from}, :resume, state) do
    {:keep_state, state(state, paused: false), [{:reply, from, :ok}]}
  end

  def connected({:call, from}, :clientid, state(clientid: client_id)) do
    {:keep_state_and_data, [{:reply, from, client_id}]}
  end

  def connected({:call, from}, {:subscribe, properties, topics} = sub_req,
                state(last_packet_id: packet_id, subscriptions: subscriptions) = state) do
    case send_data(Packet.subscribe_packet(packet_id, properties, topics), state) do
      {:ok, new_state} ->
        call = new_call({:subscribe, packet_id}, from, sub_req)
        subscriptions1 = List.foldl(topics, subscriptions, fn ({topic, opts}, acc) ->
          Map.put(acc, topic, opts)
        end)
        {:keep_state, ensure_ack_timer(add_call(call, state(new_state, subscriptions: subscriptions1)))}
      {:error, reason} = error ->
        {:stop_and_reply, reason, [{:reply, from, error}]}
    end
  end

  def connected({:call, from}, {:publish, %Packet.Msg{qos: Const.qos_0} = msg}, state) do
    case send_data(msg, state) do
      {:ok, new_state} ->
        {:keep_state, new_state, [{:reply, from, :ok}]}
      {:error, reason} = error ->
        {:stop_and_reply, reason, [{:reply, from, error}]}
    end
  end

  def connected({:call, from}, {:publish, %Packet.Msg{qos: qos} = msg},
                state(inflight: inflight, last_packet_id: packet_id) = state)
                when qos === Const.qos_1 or qos === Const.qos_2 do
    msg1 = %Packet.Msg{msg | packet_id: packet_id}
    case send_data(msg1, state) do
      {:ok, new_state} ->
        inflight1 = Map.put(inflight, packet_id, {:publish, msg1, :os.timestamp()})
        state1 = ensure_retry_timer(state(new_state, inflight: inflight1))
        actions = [{:reply, from, {:ok, packet_id}}]
        case is_inflight_full(state1) do
            true -> {:next_state, :inflight_full, state1, actions}
            false -> {:keep_state, state1, actions}
        end
      {:error, reason} ->
        {:stop_and_reply, reason, [{:reply, from, {:error, {packet_id, reason}}}]}
    end
  end

  def connected({:call, from}, {:unsubscribe, properties, topics} = unsubreq,
                 state(last_packet_id: packet_id) = state) do
    case send_data(Packet.unsubscribe_packet(packet_id, properties, topics), state) do
      {:ok, new_state} ->
        call = new_call({:unsubscribe, packet_id}, from, unsubreq)
        {:keep_state, ensure_ack_timer(add_call(call, new_state))}
      {:error, reason} = error ->
        {:stop_and_reply, reason, [{:reply, from, error}]}
    end
  end

  def connected({:call, from}, :ping, state) do
    case send_data(Packet.packet(Const.pingreq), state) do
      {:ok, new_state} ->
        call = new_call(:ping, from)
        {:keep_state, ensure_ack_timer(add_call(call, new_state))}
      {:error, reason} = error ->
        {:stop_and_reply, reason, [{:reply, from, error}]}
    end
  end

  def connected({:call, from}, {:disconnect, reason_code, properties}, state) do
    case send_data(Packet.disconnect_packet(reason_code, properties), state) do
      {:ok, new_state} ->
        {:stop_and_reply, :normal, [{:reply, from, :ok}], new_state}
      {:error, reason} = error ->
        {:stop_and_reply, reason, [{:reply, from, error}]}
    end
  end

  def connected(:cast, {:puback, packet_id, reason_code, properties}, state) do
    send_puback(Packet.puback_packet(packet_id, reason_code, properties), state)
  end

  def connected(:cast, {:pubrec, packet_id, reason_code, properties}, state) do
    send_puback(Packet.pubrec_packet(packet_id, reason_code, properties), state)
  end

  def connected(:cast, {:pubrel, packet_id, reason_code, properties}, state) do
    send_puback(Packet.pubrel_packet(packet_id, reason_code, properties), state)
  end

  def connected(:cast, {:pubcomp, packet_id, reason_code, properties}, state) do
    send_puback(Packet.pubcomp_packet(packet_id, reason_code, properties), state)
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{
        type: Const.publish(),
        qos: _qos
      },
      variable: %Packet.Publish{}
    },
    state(paused: true)) do
    :keep_state_and_data
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{
        type: Const.publish(),
        qos: Const.qos_0
      },
      variable: %Packet.Publish{}
    } = packet,
    state) do
    {:keep_state, deliver(packet_to_msg(packet), state)}
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{
        type: Const.publish(),
        qos: Const.qos_1
      },
      variable: %Packet.Publish{}
    } = packet,
    state) do
    publish_process(Const.qos_1, packet, state)
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{
        type: Const.publish(),
        qos: Const.qos_2
      },
      variable: %Packet.Publish{}
    } = packet,
    state) do
    publish_process(Const.qos_2, packet, state)
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.puback()},
      variable: _
    } = puback,
    state) do
    {:keep_state, delete_inflight(puback, state)}
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.pubrec()},
      variable: %Packet.Puback{
        packet_id: packet_id,
        reason_code: 0
      }
    },
    state(inflight: inflight) = state) do
    n_state = case Map.fetch(inflight, packet_id) do
          {:ok, {:publish, _msg, _ts}} ->
            inflight1 = Map.put(inflight, packet_id, {:pubrel, packet_id, :os.timestamp()})
            state(state, inflight: inflight1)
          {:ok, {:pubrel, _ref, _ts}} ->
            Logger.warn("Duplicated PUBREC Packet #{inspect(packet_id)}")
            state
          :error ->
            Logger.error("Unexpected PUBREC Packet #{inspect(packet_id)}")
            state
        end
      send_puback(Packet.pubrel_packet(packet_id), n_state)
  end

  # TODO::... if auto_ack is false, should we take packet_id from the map?
  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{
        type: Const.pubrel(),
        qos: Const.qos_1()
      },
      variable: %Packet.Puback{
        packet_id: packet_id,
        reason_code: 0
      }
    },
    state(awaiting_rel: awaiting_rel, auto_ack: auto_ack) = state) do
    case map_take(awaiting_rel, packet_id) do
      {packet, awaiting_rel1} ->
        new_state = deliver(packet_to_msg(packet), state(state, awaiting_rel: awaiting_rel1))
        case auto_ack do
          true  -> send_puback(Packet.pubcomp_packet(packet_id), new_state)
          false -> {:keep_state, new_state}
        end
      :error ->
        Logger.warn("Unexpected PUBREL #{inspect(packet_id)}")
        :keep_state_and_data
    end
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.pubcomp()},
      variable: %Packet.Puback{}
    } = pubcomp,
    state) do
    {:keep_state, delete_inflight(pubcomp, state)}
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.suback()},
      variable: %Packet.Suback{
        packet_id: packet_id,
        properties: properties,
        reason_code: reason_code
      }
    },
    state(subscriptions: _subscriptions) = state) do
    case take_call({:subscribe, packet_id}, state) do
      {:value, call(from: from), new_state} ->
        # TODO: Merge reason codes to subscriptions?
        reply = {:ok, properties, reason_code}
        {:keep_state, new_state, [{:reply, from, reply}]}
      false ->
        :keep_state_and_data
    end
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.unsuback()},
      variable: %Packet.Unsuback{
        packet_id: packet_id,
        properties: properties,
        reason_code: reason_code
      }
    },
    state(subscriptions: subscriptions) = state) do
    case take_call({:unsubscribe, packet_id}, state) do
      {:value, call(from: from, req: {_, _, topics}), new_state} ->
        subscriptions1 = List.foldl(topics, subscriptions, fn (topic, acc) ->
          Map.delete(acc, topic)
        end)
        {:keep_state, state(new_state, subscriptions: subscriptions1),
          [{:reply, from, {:ok, properties, reason_code}}]}
      false ->
        :keep_state_and_data
    end
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.pingresp}
    },
    state(pending_calls: [])) do
    :keep_state_and_data
  end
  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.pingresp}
    },
    state) do
    case take_call(:ping, state) do
      {:value, call(from: from), new_state} ->
        {:keep_state, new_state, [{:reply, from, :pong}]}
      false ->
        :keep_state_and_data
    end
  end

  def connected(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.disconnect()},
      variable: %Packet.Disconnect{
        reason_code: reason_code,
        properties: properties
      }
    },
    state) do
    {:stop, {:disconnected, reason_code, properties}, state}
  end

  def connected(:info, {:timeout, _tref, :keepalive}, state(force_ping: true) = state) do
    case send_data(Packet.packet(Const.pingreq), state) do
      {:ok, new_state} ->
          {:keep_state, ensure_keepalive_timer(new_state)}
      error -> {:stop, error}
    end
  end

  def connected(:info, {:timeout, tref, :keepalive},
                state(conn_mod: conn_mod, socket: sock,
                paused: paused, keepalive_timer: tref) = state) do
    case (not paused) and should_ping(conn_mod, sock) do
      true ->
        case send_data(Packet.packet(Const.pingreq), state) do
          {:ok, new_state} ->
            {:keep_state, ensure_keepalive_timer(new_state), [:hibernate]}
          error -> {:stop, error}
        end
      false ->
        {:keep_state, ensure_keepalive_timer(state), [:hibernate]}
      {:error, reason} ->
        {:stop, reason}
    end
  end

  def connected(:info, {:timeout, tref, :ack}, state(ack_timer: tref,
                                                     ack_timeout: timeout,
                                                     pending_calls: calls) = state) do
    new_state = state(state, ack_timer: nil, pending_calls: timeout_calls(timeout, calls))
    {:keep_state, ensure_ack_timer(new_state)}
  end

  def connected(:info, {:timeout, tref, :retry}, state(retry_timer: tref, inflight: inflight) = state) do
    case map_size(inflight) == 0 do
      true  -> {:keep_state, state(state, retry_timer: nil)}
      false -> retry_send(state)
    end
  end

  def connected(event_type, event_content, data) do
    handle_event(event_type, event_content, :connected, data)
  end

  def inflight_full({:call, _from}, {:publish, %Packet.Msg{qos: qos}}, _state)
    when (qos === Const.qos_1) or (qos === Const.qos_2) do
    {:keep_state_and_data, [:postpone]}
  end
  def inflight_full(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.puback()},
      variable: %Packet.Puback{}
    } = puback,
    state) do
    delete_inflight_when_full(puback, state)
  end
  def inflight_full(
    :cast,
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.pubcomp()},
      variable: %Packet.Puback{}
    } = pubcomp,
    state) do
      delete_inflight_when_full(pubcomp, state)
  end
  def inflight_full(event_type, event_content, data) do
    # inflight_full is a sub-state of connected state,
    # delegate all other events to connected state.
    connected(event_type, event_content, data)
  end

  def handle_event({:call, from}, :stop, _state_name, _State) do
    {:stop_and_reply, :normal, [{:reply, from, :ok}]}
  end

  def handle_event(:info, {:gun_ws, conn_pid, _stream_ref, {:binary, data}},
                   _state_name, state(socket: conn_pid) = state) do
    Logger.debug("RECV Data #{inspect(data)}")
    process_incoming(:erlang.iolist_to_binary(data), [], state)
  end

  def handle_event(:info, {:gun_down, conn_pid, _, reason, _, _},
                   _state_name, state(socket: conn_pid) = state) do
    Logger.debug("WebSocket down! Reason #{inspect(reason)}")
    {:stop, reason, state}
  end

  def handle_event(:info, {tcp_or_ssL, _sock, data}, _state_name, state)
    when tcp_or_ssL === :tcp or tcp_or_ssL === :ssl do
    Logger.debug("RECV Data #{inspect(data)}")
    process_incoming(data, [], run_sock(state))
  end

  def handle_event(:info, {error, _sock, reason}, _state_name, state)
    when error === :tcp_error or error === :ssl_error do
    Logger.error("client ID : #{inspect(state(state, :clientid))} ->
      The connection error occured #{inspect(error)}, reason: #{inspect(reason)}")
    {:stop, {:shutdown, reason}, state}
  end

  def handle_event(:info, {closed, _sock}, _state_name, state)
    when closed === :tcp_closed or closed === :ssl_closed do
    Logger.debug("Close #{inspect(closed)}")
    {:stop, {:shutdown, closed}, state}
  end

  def handle_event(:info, {"EXIT", owner, reason}, _, state(owner: owner) = state) do
    Logger.debug("Got EXIT from owner, Reason #{inspect(reason)}")
    {:stop, {:shutdown, reason}, state}
  end

  def handle_event(:info, {:inet_reply, _sock, :ok}, _, _state) do
    :keep_state_and_data
  end

  def handle_event(:info, {:inet_reply, _sock, {:error, reason}}, _, state) do
    Logger.error("Got tcp error #{inspect(reason)}")
    {:stop, {:shutdown, reason}, state}
  end

  def handle_event(:info, {"EXIT", _pid, :normal} = event_content, state_name, _state) do
    Logger.info("state: #{inspect(state_name)}, Unexpected Event: #{inspect(event_content)}")
    :keep_state_and_data
  end

  def handle_event(event_type, event_content, state_name, _state) do
    Logger.info("state: #{inspect(state_name)},
      Unexpected Event: (#{inspect(event_type)}, #{inspect(event_content)})")
    :keep_state_and_data
  end

  # Mandatory callback functions
  def terminate(reason, _state_name, state(conn_mod: conn_mod, socket: socket) = state) do
    case reason do
      {:disconnected, reason_code, properties} ->
        # backward compatible
        :ok = eval_msg_handler(state, :disconnected, {reason_code, properties})
      _ ->
        :ok = eval_msg_handler(state, :disconnected, reason)
    end
    case socket === nil do
      true -> :ok
      _ -> conn_mod.close(socket)
    end
  end

  def code_change(_vsn, state, data, _extra) do
    {:ok, state, data}
  end


  # -----------------------------------------------------------------
  # Internal functions
  # -----------------------------------------------------------------
  defp should_ping(conn_mod, sock) do
    case conn_mod.getstat(sock, [:send_oct]) do
      {:ok, [{:send_oct, val}]} ->
          old_val = Process.get(:send_oct)
          Process.put(:send_oct, val)
          old_val == nil or old_val == val
      {:error, _reason} = error ->
          error
    end
  end

  defp is_inflight_full(state(max_inflight: :infinity)) do
    false
  end
  defp is_inflight_full(state(max_inflight: max_limit, inflight: inflight)) do
    map_size(inflight) >= max_limit
  end

  defp delete_inflight(
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.puback()},
      variable: %Packet.Puback{
        packet_id: packet_id,
        reason_code: reason_code,
        properties: properties
      }
    },
    state(inflight: inflight) = state) do
    case Map.fetch(inflight, packet_id) do
      {:ok, {:publish, %Packet.Msg{packet_id: packet_id}, _ts}} ->
        :ok = eval_msg_handler(state, :puback, %{packet_id: packet_id,
                                                reason_code: reason_code,
                                                properties: properties})
        state(state, inflight: Map.delete(inflight, packet_id))
      :error ->
        Logger.warn("Unexpected PUBACK Packet #{inspect(packet_id)}")
        state
      end
  end
  defp delete_inflight(
    %Packet.Mqtt{
      header: %Packet.Header{type: Const.pubcomp()},
      variable: %Packet.Puback{
        packet_id: packet_id,
        reason_code: reason_code,
        properties: properties
      }
    },
    state(inflight: inflight) = state) do
      case Map.fetch(inflight, packet_id) do
        {:ok, {:pubrel, _packet_id, _ts}} ->
          :ok = eval_msg_handler(state, :puback, %{packet_id: packet_id,
                                                  reason_code: reason_code,
                                                  properties: properties})
          state(state, inflight: Map.delete(inflight, packet_id))
        :error ->
        Logger.warn("Unexpected PUBCOMP Packet #{inspect(packet_id)}")
        state
      end
  end

  defp delete_inflight_when_full(packet, state) do
    state1 = delete_inflight(packet, state)
    case is_inflight_full(state1) do
      true -> {:keep_state, state1}
      false -> {:next_state, :connected, state1}
    end
  end

  defp assign_id(Const.no_client_id, props) do
    case Map.fetch(props, "Assigned-client-Identifier") do
      {:ok, value} ->
        value
      :error ->
        raise Errors.BadClientID
    end
  end
  defp assign_id(id, _props) do
    id
  end

  defp publish_process(
    Const.qos_1,
    %Packet.Mqtt{
      header: %Packet.Header{
        type: Const.publish(),
        qos: Const.qos_1
      },
      variable: %Packet.Publish{packet_id: packet_id}
    } = packet,
    state(auto_ack: auto_ack) = state0) do
    state = deliver(packet_to_msg(packet), state0)
    case auto_ack do
      true  -> send_puback(Packet.puback_packet(packet_id), state)
      false -> {:keep_state, state}
    end
  end
  defp publish_process(
    Const.qos_2,
    %Packet.Mqtt{
      header: %Packet.Header{
        type: Const.publish(),
        qos: Const.qos_2
      },
      variable: %Packet.Publish{packet_id: packet_id}
    } = packet,
    state(awaiting_rel: awaiting_rel) = state) do
    case send_puback(Packet.pubrec_packet(packet_id), state) do
        {:keep_state, new_state} ->
            awaiting_rel1 = Map.put(awaiting_rel, packet_id, packet)
            {:keep_state, state(new_state, awaiting_rel: awaiting_rel1)}
        stop -> stop
    end
  end

  defp ensure_keepalive_timer(state(properties: %{"Server-Keep-Alive": secs}) = state) do
    ensure_keepalive_timer(:timer.seconds(secs), state(state, keepalive: secs))
  end
  defp ensure_keepalive_timer(state(keepalive: 0) = state) do
    state
  end
  defp ensure_keepalive_timer(state(keepalive: i) = state) do
    ensure_keepalive_timer(:timer.seconds(i), state)
  end
  defp ensure_keepalive_timer(i, state) when is_integer(i) do
    state(state, keepalive_timer: :erlang.start_timer(i, self(), :keepalive))
  end

  defp new_call(id, from) do
    new_call(id, from, nil)
  end
  defp new_call(id, from, req) do
    call(id: id, from: from, req: req, ts: :os.timestamp())
  end

  defp add_call(call, state(pending_calls: calls) = data) do
    state(data, pending_calls: [call | calls])
  end

  defp take_call(id, state(pending_calls: calls) = data) do
    # REVIEW: id equals 1 in elixir, and equals 2 in erlang
    case List.keytake(calls, id, call(:id)) do
      {call, left} ->
        {:value, call, state(data, pending_calls: left)}
      nil ->
        false
    end
  end

  defp timeout_calls(timeout, calls) do
    timeout_calls(:os.timestamp(), timeout, calls)
  end
  defp timeout_calls(now, timeout, calls) do
    List.foldl(calls, [], fn (call(from: from, ts: ts) = c, acc) ->
      case div(:timer.now_diff(now, ts), 1000) >= timeout do
          true  ->
            send(from, {:error, :ack_timeout})
            acc
          false -> [c | acc]
      end
    end)
  end

  defp ensure_ack_timer(state(ack_timer: nil,
                      ack_timeout: timeout,
                      pending_calls: calls) = state) when length(calls) > 0 do
    state(state, ack_timer: :erlang.start_timer(timeout, self(), :ack))
  end
  defp ensure_ack_timer(state), do: state

  defp ensure_retry_timer(state(retry_interval: interval) = state) do
    do_ensure_retry_timer(interval, state)
  end

  defp do_ensure_retry_timer(interval, state(retry_timer: nil) = state) when interval > 0 do
    state(state, retry_timer: :erlang.start_timer(interval, self(), :retry))
  end
  defp do_ensure_retry_timer(_interval, state) do
    state
  end

  defp retry_send(state(inflight: inflight) = state) do
    msgs = Enum.sort_by(inflight, &(elem(&1, 2)))
    retry_send(msgs, :os.timestamp(), state)
  end

  defp retry_send([], _now, state) do
    {:keep_state, ensure_retry_timer(state)}
  end
  defp retry_send([{type, msg, ts} | msgs], now, state(retry_interval: interval) = state) do
    # micro -> ms
    diff = div(:timer.now_diff(now, ts), 1000)
    case diff >= interval do
        true ->
          case retry_send(type, msg, now, state) do
            {:ok, new_state} -> retry_send(msgs, now, new_state)
            {:error, error} -> {:stop, error}
          end
        false ->
          {:keep_state, do_ensure_retry_timer(interval - diff, state)}
    end
  end

  defp retry_send(:publish, %Packet.Msg{qos: qos, packet_id: packet_id} = msg,
                 now, state(inflight: inflight) = state) do
    msg1 = %Packet.Msg{msg | dup: (qos === Const.qos_1)}
    case send_data(msg1, state) do
        {:ok, new_state} ->
            inflight1 = Map.put(inflight, packet_id, {:publish, msg1, now})
            {:ok, state(new_state, inflight: inflight1)}
        {:error, _reason} = error ->
            error
    end
  end

  defp retry_send(:pubrel, packet_id, now, state(inflight: inflight) = state) do
    case send_data(Packet.pubrel_packet(packet_id), state) do
        {:ok, new_state} ->
            inflight1 = Map.put(inflight, packet_id, {:pubrel, packet_id, now})
            {:ok, state(new_state, inflight: inflight1)}
        {:error, _reason} = error ->
            error
    end
  end

  defp deliver(%Packet.Msg{qos: qos, dup: dup, retain: retain, packet_id: packet_id,
              topic: topic, props: props, payload: payload}, state) do
    msg = %{qos: qos, dup: dup, retain: retain, packet_id: packet_id, topic: topic,
            properties: props, payload: payload, client_pid: self()}
    :ok = eval_msg_handler(state, :publish, msg)
    state
  end

  defp eval_msg_handler(state(msg_handler: Const.no_msg_hdlr, owner: owner),
   :disconnected, {reason_code, properties}) do
      # Special handling for disconnected message when there is no handler callback
      send(owner, {:disconnected, reason_code, properties})
      :ok
  end
  defp eval_msg_handler(state(msg_handler: Const.no_msg_hdlr), :disconnected, _other_reason) do
    # do nothing to be backward compatible
    :ok
  end

  defp eval_msg_handler(state(msg_handler: Const.no_msg_hdlr, owner: owner), kind, msg) do
    send(owner, {kind, msg})
    :ok
  end
  defp eval_msg_handler(state(msg_handler: handler), kind, msg) do
    f = Map.get(handler, kind)
    _ = f.(msg)
    :ok
  end

  defp packet_to_msg(%Packet.Mqtt{header: %Packet.Header{type: Const.publish,
                                                        dup: dup,
                                                        qos: qos,
                                                        retain: r},
                                variable: %Packet.Publish{topic_name: topic,
                                                           packet_id: packet_id,
                                                           properties: props},
                                payload: payload}) do
    %Packet.Msg{qos: qos, retain: r, dup: dup, packet_id: packet_id, topic: topic,
                props: props, payload: payload}

  end

  defp msg_to_packet(%Packet.Msg{qos: qos, dup: dup, retain: retain, packet_id: packet_id,
                                topic: topic, props: props, payload: payload}) do
    %Packet.Mqtt{header: %Packet.Header{type: Const.publish,
                                        qos: qos,
                                        retain: retain,
                                        dup: dup},
                variable: %Packet.Publish{topic_name: topic,
                                  packet_id: packet_id,
                                  properties: props},
                payload: payload}
  end

  # -----------------------------------------------------------------
  # Socket Connect/Send
  # -----------------------------------------------------------------
  def sock_connect(conn_mod, hosts, sock_opts, timeout) do
      sock_connect(conn_mod, hosts, sock_opts, timeout, {:error, :no_hosts})
  end

  def sock_connect(_conn_mod, [], _sock_opts, _timeout, last_err) do
    last_err
  end
  def sock_connect(conn_mod, [{host, port} | hosts], sock_opts, timeout, _last_err) do
    case conn_mod.connect(host, port, sock_opts, timeout) do
      {:ok, sock_or_pid} ->
        {:ok, sock_or_pid}
      {:error, _reason} = error ->
          sock_connect(conn_mod, hosts, sock_opts, timeout, error)
    end
  end

  def hosts(state(hosts: [], host: host, port: port)) do
    [{host, port}]
  end
  def hosts(state(hosts: hosts)) do
    hosts
  end

  def send_puback(packet, state) do
    case send_data(packet, state) do
      {:ok, new_state}  -> {:keep_state, new_state}
      {:error, reason} -> {:stop, {:shutdown, reason}}
    end
  end

  def send_data(%Packet.Msg{} = msg, state) do
    send_data(msg_to_packet(msg), state)
  end

  def send_data(%Packet.Mqtt{} = packet, state(conn_mod: conn_mod, socket: sock, proto_ver: ver) = state) do
    data = Frame.serialize(packet, ver)
    Logger.debug("SEND Data #{inspect(packet)}")
    case conn_mod.send_data(sock, data) do
        :ok -> {:ok, bump_last_packet_id(state)}
        error -> error
    end
  end

  def run_sock(state(conn_mod: conn_mod, socket: sock) = state) do
    conn_mod.setopts(sock, [{:active, :once}])
    state
  end

  # -----------------------------------------------------------------
  # Process incomming
  # -----------------------------------------------------------------
  def process_incoming(<<>>, packets, state) do
    {:keep_state, state, next_events(packets)}
  end

  def process_incoming(bytes, packets, state(parse_state: parse_state) = state) do
    # try Frame.parse(bytes, parse_state) do
    try do
      case Frame.parse(bytes, parse_state) do
        {:ok, packet, rest, n_parse_state} ->
          process_incoming(rest, [packet | packets], state(state, parse_state: n_parse_state))
        {:more, n_parse_state} ->
          {:keep_state, state(state, parse_state: n_parse_state), next_events(packets)}
      end
    rescue
        error -> {:stop, error}
    end
  end

  @compile {:inline, next_events: 1}
  def next_events([]) do
    []
  end
  def next_events([packet]) do
    {:next_event, :cast, packet}
  end
  def next_events(packets) do
    for packet <- Enum.reverse(packets) do
      {:next_event, :cast, packet}
    end
  end


  # -----------------------------------------------------------------
  # packet_id generation
  # -----------------------------------------------------------------
  def bump_last_packet_id(state(last_packet_id: id) = state) do
    state(state, last_packet_id: next_packet_id(id))
  end

  @spec next_packet_id(packet_id()) :: packet_id()
  def next_packet_id(Const.max_packet_id), do: 1
  def next_packet_id(id), do: id + 1


  # -----------------------------------------------------------------
  # reason_code Name
  # -----------------------------------------------------------------
  defp reason_code_name(i, ver) when ver >= Const.mqtt_proto_v5 do
    reason_code_name(i)
  end
  defp reason_code_name(0, _ver), do: :connection_acceptd
  defp reason_code_name(1, _ver), do: :unacceptable_protocol_version
  defp reason_code_name(2, _ver), do: :client_identifier_not_valid
  defp reason_code_name(3, _ver), do: :server_unavaliable
  defp reason_code_name(4, _ver), do: :malformed_username_or_password
  defp reason_code_name(5, _ver), do: :unauthorized_client
  defp reason_code_name(_, _ver), do: :unknown_error

  defp reason_code_name(0x00), do: :success
  defp reason_code_name(0x01), do: :granted_qos1
  defp reason_code_name(0x02), do: :granted_qos2
  defp reason_code_name(0x04), do: :disconnect_with_will_message
  defp reason_code_name(0x10), do: :no_matching_subscribers
  defp reason_code_name(0x11), do: :no_subscription_existed
  defp reason_code_name(0x18), do: :continue_authentication
  defp reason_code_name(0x19), do: :re_authenticate
  defp reason_code_name(0x80), do: :unspecified_error
  defp reason_code_name(0x81), do: :malformed_Packet
  defp reason_code_name(0x82), do: :protocol_error
  defp reason_code_name(0x83), do: :implementation_specific_error
  defp reason_code_name(0x84), do: :unsupported_protocol_version
  defp reason_code_name(0x85), do: :client_identifier_not_valid
  defp reason_code_name(0x86), do: :bad_username_or_password
  defp reason_code_name(0x87), do: :not_authorized
  defp reason_code_name(0x88), do: :server_unavailable
  defp reason_code_name(0x89), do: :server_busy
  defp reason_code_name(0x8A), do: :banned
  defp reason_code_name(0x8B), do: :server_shutting_down
  defp reason_code_name(0x8C), do: :bad_authentication_method
  defp reason_code_name(0x8D), do: :keepalive_timeout
  defp reason_code_name(0x8E), do: :session_taken_over
  defp reason_code_name(0x8F), do: :topic_filter_invalid
  defp reason_code_name(0x90), do: :topic_name_invalid
  defp reason_code_name(0x91), do: :packet_identifier_inuse
  defp reason_code_name(0x92), do: :packet_identifier_not_found
  defp reason_code_name(0x93), do: :receive_maximum_exceeded
  defp reason_code_name(0x94), do: :topic_alias_invalid
  defp reason_code_name(0x95), do: :packet_too_large
  defp reason_code_name(0x96), do: :message_rate_too_high
  defp reason_code_name(0x97), do: :quota_exceeded
  defp reason_code_name(0x98), do: :administrative_action
  defp reason_code_name(0x99), do: :payload_format_invalid
  defp reason_code_name(0x9A), do: :retain_not_supported
  defp reason_code_name(0x9B), do: :qos_not_supported
  defp reason_code_name(0x9C), do: :use_another_server
  defp reason_code_name(0x9D), do: :server_moved
  defp reason_code_name(0x9E), do: :shared_subscriptions_not_supported
  defp reason_code_name(0x9F), do: :connection_rate_exceeded
  defp reason_code_name(0xA0), do: :maximum_connect_time
  defp reason_code_name(0xA1), do: :subscription_identifiers_not_supported
  defp reason_code_name(0xA2), do: :wildcard_subscriptions_not_supported
  defp reason_code_name(_code), do: :unknown_error

  # -----------------------------------------------------------------
  # Helper
  # -----------------------------------------------------------------
  defp with_owner(options) do
    case Keyword.get(options, :owner) do
      owner when is_pid(owner) ->
        options
      nil ->
        [{:owner, self()} | options]
    end
  end

  defp random_client_id() do
    :rand.seed(:exsplus, :erlang.timestamp())
    i1 = :rand.uniform(round(:math.pow(2, 48))) - 1
    i2 = :rand.uniform(round(:math.pow(2, 32))) - 1
    {:ok, host} = :inet.gethostname()
    rand_id = :io_lib.format("~12.16.0b~8.16.0b", [i1, i2])
    :erlang.iolist_to_binary(["exmqtt-", host, "-", rand_id])
  end

  defp merge_opts(defaults, options) do
    List.foldl(options, defaults, fn
      {opt, val}, acc ->
        List.keystore(acc, opt, 0, {opt, val})
      opt, acc ->
        [opt | acc] |> Enum.uniq() |> Enum.sort()
      end)
  end


  @compile {:inline, qos_i: 1}
  defp qos_i(name) do
    case name do
      Const.qos_0    -> Const.qos_0
      :qos0          -> Const.qos_0
      :at_most_once  -> Const.qos_0
      Const.qos_1    -> Const.qos_1
      :qos1          -> Const.qos_1
      :at_least_once -> Const.qos_1
      Const.qos_2    -> Const.qos_2
      :qos2          -> Const.qos_2
      :exactly_once  -> Const.qos_2
    end
  end

  defp record_info(record) do
    state(record) |> Keyword.keys()
  end

  defp map_take(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        {value, Map.delete(map, key)}
      :error ->
        :error
    end
  end
end
