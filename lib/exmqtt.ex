defmodule Exmqtt do
  require Record
  require ExmqttConstants
  alias ExmqttConstants, as: Const
  alias Exmqtt.Packet
  alias Exmqtt.Frame
  alias Exmqtt.Props
  alias Exmqtt.Sock
  alias Exmqtt.Ws

  @type host() :: :inet.ip_address() | :inet.hostname()
  @type maybe(t) :: nil | t
  @type topic() :: binary()
  @type payload() :: iodata()
  @type packet_id() :: 0..0xFF
  @type reason_code() :: 0..0xFF
  @type properties() :: %{String.t() => term()}
  @type version() :: Const.mqtt_proto_v3 | Const.mqtt_proto_v4 | Const.mqtt_proto_v5
  @type qos() :: Const.qos_0 | const.qos_1 | Const.qos_2
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
  @type conn_mod() :: :emqtt_sock | :emqtt_ws
  @type client() :: pid() | atom()
  @opaque mqtt_msg() :: %mqtt_msg{}

  # Message handler is a set of callbacks defined to handle MQTT messages
  # as well as the disconnect event.
  @type msg_handler() :: %{
    puback: function() :: any(),
    publish: function(:emqx_types.message()) :: any(),
    disconnected: function({reason_code(), _properties :: term()}) :: any()
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
                  | {:ws_path, string()}
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
    max_inflight:    infinity | pos_integer(),
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
        GenStateMachine.start_link(__MUDULE__, [with_owner(options)], name: reg_name)
    end
  end

  # -----------------------------------------------------------------
  # Helper
  # -----------------------------------------------------------------
  defp property(name, val), do: state(properties: %{name => val})
  defp will_msg(qos, retain, topic, props, payload) do
    %Packet.Msg{
      qos: qos,
		  retain: retain,
		  topic: topic,
		  props: props,
		  payload: payload
    }
  end

  defp with_owner(options) do
    case Keyword.get(options, :owner) do
      owner when is_pid(owner) ->
        options
      nil ->
        [{:owner, self()} | options]
    end
  end

end
