defmodule Exmqtt.Frame do
  require ExmqttConstants
  alias ExmqttConstants, as: Const
  alias Exmqtt.Packet

  @type version() :: Const.mqtt_proto_v3() | Const.mqtt_proto_v4() | Const.mqtt_proto_v5()

  @default_options %{
    strict_mode: false,
    max_size: Const.max_packet_size(),
    version: Const.mqtt_proto_v4()
  }

  # -------------------------------------------------
  # Init Parse State
  # -------------------------------------------------
  @spec initial_parse_state :: {:none, %{max_size: any, strict_mode: any, version: any}}
  def initial_parse_state(), do: initial_parse_state(%{})
  @spec initial_parse_state(map) :: {:none, %{max_size: any, strict_mode: any, version: any}}
  def initial_parse_state(options) when is_map(options) do
    options
    |> merge_opts
    |> none
  end

  # -------------------------------------------------
  # Parse MQTT Frame
  # -------------------------------------------------
  def parse(bin), do: parse(bin, initial_parse_state())

  def parse(<<>>, {:none, options}) do
    {:more, fn bin -> parse(bin, {:none, options}) end}
  end

  def parse(
        <<type::4, dup::1, qos::2, retain::1, rest::binary>>,
        {:none, %{strict_mode: strict_mode} = options}
      ) do
    # Validate header if strict mode
    strict_mode and validate_header(type, dup, qos, retain)
    header = %Packet.Header{
      type: type,
      dup: bool(dup),
      qos: qos,
      retain: bool(retain)
    }
    # TODO: when does match `fixed_qos`
    header1 = case fixqos(type, qos) do
      _qos -> header
      fixed_qos -> %{header | qos: fixed_qos}
    end
    parse_remaining_len(rest, header1, options)
  end
  def parse(bin, cont) when is_binary(bin) and is_function(cont) do
    cont.(bin)
  end

  def parse_remaining_len(<<>>, header, options) do
    {:more, fn bin -> parse_remaining_len(bin, header, options) end}
  end
  def parse_remaining_len(rest, header, options) do
    parse_remaining_len(rest, header, 1, 0, options)
  end

  def arse_remaining_len(
    _bin,
    _header,
    _multiplier,
    length,
    %{max_size: max_size}) when length > max_size do
      raise "mqtt frame too large"
  end

  def parse_remaining_len(<<>>, header, multiplier, length, options) do
    {:more, fn bin -> parse_remaining_len(bin, header, multiplier, length, options) end}
  end
  # Match DISCONNECT without payload
  def parse_remaining_len(
    <<0::8, rest::binary>>,
    %Packet.Header{type: Const.disconnect} = header,
    1, 0, options) do
      packet = mqtt_packet(header, %Packet.Disconnect{reason_code: Const.rc_success})
      {:ok, packet, rest, none(options)}
  end

  # Match PINGREQ
  def parse_remaining_len(<<0::8, rest::binary>>, header, 1, 0, options) do
    parse_frame(rest, header, 0, options)
  end

  # Match PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK...
  def parse_remaining_len(<<0::1, 2::7, rest::binary>>, header, 1, 0, options) do
    parse_frame(rest, header, 2, options)
  end

  def parse_remaining_len(<<1::1, len::7, rest::binary>>, header, multiplier, value, options) do
    parse_remaining_len(rest, header, multiplier * Const.highbit, value + len * multiplier, options)
  end
  def parse_remaining_len(<<0::1, len::7, rest::binary>>, header, multiplier, value,
    %{max_size: max_size} = options) do
    frame_len = value + len * multiplier
    if frame_len > max_size do
      raise "mqtt frame too large"
    else
      parse_frame(rest, header, frame_len, options)
    end
  end

  def parse_frame(bin, header, 0, options) do
    {:ok, mqtt_packet(header), bin, none(options)}
  end

  def parse_frame(bin, header, length, options) do
    case bin do
      <<frame_bin::binary-size(length), rest::binary>> ->
        case parse_packet(header, frame_bin, options) do
          {variable, payload} ->
            {:ok, mqtt_packet(header, variable, payload), rest, none(options)}
          %Packet.Connect{proto_ver: ver} = variable ->
            {:ok, mqtt_packet(header, variable), rest, none(%{options | version: ver})}
          variable ->
            {:ok, mqtt_packet(header, variable), rest, none(options)}
        end
      too_short_bin ->
        {:more, fn bin_more -> parse_frame(<<too_short_bin::binary, bin_more::binary>>, header, length, options) end}
    end
  end

  def parse_packet(%Packet.Header{type: Const.connect}, frame_bin, _options) do
    {proto_name, rest} = parse_utf8_string(frame_bin)
    <<bridge_tag::4, proto_ver::4, rest1::binary>> = rest
    # Note: Crash when reserved flag doesn't equal to 0, there is no strict compliance with the MQTT5.0
    <<username_flag::1,
      password_flag::1,
      will_retain::1,
      will_qoS::2,
      will_flag::1,
      clean_start::1,
      0::1,
      keepalive::16,
      rest2::binary>> = rest1

    {properties, rest3} = parse_properties(rest2, proto_ver)
    {clientid, rest4} = parse_utf8_string(rest3)
    conn_packet = %Packet.Connect{
      proto_name: proto_name,
      proto_ver: proto_ver,
      is_bridge: (bridge_tag === 8),
      clean_start: bool(clean_start),
      will_flag: bool(will_flag),
      will_qos: will_qoS,
      will_retain: bool(will_retain),
      keepalive: keepalive,
      properties: properties,
      clientid: clientid}
    {conn_packet1, rest5} = parse_will_message(conn_packet, rest4)
    {username, rest6} = parse_utf8_string(rest5, bool(username_flag))
    {password, <<>>} = parse_utf8_string(rest6, bool(password_flag))
    %{conn_packet1 | username: username, password: password}
  end

  def parse_packet(
    %Packet.Header{type: Const.connack},
    <<ack_flags::8, reason_code::8, rest::binary>>,
    %{version: ver}) do
      {properties, <<>>} = parse_properties(rest, ver)
      %Packet.Connack{
        ack_flags: ack_flags,
        reason_code: reason_code,
        properties: properties
      }
  end

  def parse_packet(%Packet.Header{type: Const.publish, qos: qos}, bin, %{version: ver}) do
    {topic_name, rest} = parse_utf8_string(bin)
    {packet_id, rest1} = case qos do
                            Const.qos_0 -> {nil, rest}
                            _ -> parse_packet_id(rest)
                        end
    (packet_id !== nil) and validate_packet_id(packet_id)
    {properties, payload} = parse_properties(rest1, ver)
    {%Packet.Publish{
      topic_name: topic_name,
      packet_id: packet_id,
      properties: properties},
      payload}
  end

  def parse_packet(%Packet.Header{type: puback}, <<packet_id::16>>, _options)
    when Const.puback <= puback and puback <= Const.pubcomp do
      :ok = validate_packet_id(packet_id)
      %Packet.Puback{packet_id: packet_id, reason_code: 0}
  end

  def parse_packet(
    %Packet.Header{type: puback},
    <<packet_id::16, reason_code, rest::binary>>,
    %{version: Const.mqtt_proto_v5()=ver})
    when Const.puback <= puback and puback <= Const.pubcomp do
      :ok = validate_packet_id(packet_id)
      {properties, <<>>} = parse_properties(rest, ver)
      %Packet.Puback{packet_id: packet_id, reason_code: reason_code, properties: properties}
  end

  def parse_packet(
    %Packet.Header{type: Const.subscribe},
    <<packet_id::16, rest::binary>>,
    %{version: ver}) do
      :ok = validate_packet_id(packet_id)
      {properties, rest1} = parse_properties(rest, ver)
      topic_filters = parse_topic_filters(:subscribe, rest1)
      %Packet.Subscribe{
        packet_id: packet_id,
        properties: properties,
        topic_filters: topic_filters}
    end

  def parse_packet(
    %Packet.Header{type: Const.suback},
    <<packet_id::16, rest::binary>>,
    %{version: ver}) do
      :ok = validate_packet_id(packet_id)
      {properties, rest1} = parse_properties(rest, ver)
      %Packet.Suback{
        packet_id: packet_id,
        properties: properties,
        reason_code: parse_reason_codes(rest1)}
  end

  def parse_packet(
    %Packet.Header{type: Const.unsubscribe},
    <<packet_id::16, rest::binary>>,
    %{version: ver}
    ) do
      :ok = validate_packet_id(packet_id)
      {properties, rest1} = parse_properties(rest, ver)
      topic_filters = parse_topic_filters(:unsubscribe, rest1)
      %Packet.Unsubscribe{
        packet_id: packet_id,
        properties: properties,
        topic_filters: topic_filters}
  end

  def parse_packet(%Packet.Header{type: Const.unsuback}, <<packet_id::16>>, _options) do
      :ok = validate_packet_id(packet_id)
      %Packet.Unsuback{packet_id: packet_id}
  end

  def parse_packet(
    %Packet.Header{type: Const.unsuback},
    <<packet_id::16, rest::binary>>,
    %{version: ver}
    ) do
      :ok = validate_packet_id(PacketId)
      {properties, rest1} = parse_properties(rest, ver)
      reason_code = parse_reason_codes(rest1)
      %Packet.Unsuback{
        packet_id: packet_id,
        properties: properties,
        reason_code: reason_code
      }
  end

  def parse_packet(
    %Packet.Header{type: Const.disconnect},
    <<reason_code, rest::binary>>,
    %{version: Const.mqtt_proto_v5}
    ) do
      {properties, <<>>} = parse_properties(rest, Const.mqtt_proto_v5)
      %Packet.Disconnect{reason_code: reason_code, properties: properties}
  end

  def parse_packet(
    %Packet.Header{type: Const.auth},
    <<reason_code, rest::binary>>,
    %{version: Const.mqtt_proto_v5}
    ) do
      {properties, <<>>} = parse_properties(rest, Const.mqtt_proto_v5)
      %Packet.Auth{reason_code: reason_code, properties: properties}
  end

  def parse_will_message(%Packet.Connect{will_flag: true, proto_ver: ver}=packet, bin) do
    {props, rest} = parse_properties(bin, ver)
    {topic, rest1} = parse_utf8_string(rest)
    {payload, rest2} = parse_binary_data(rest1)
    {%{packet | will_props: props, will_topic: topic, will_payload: payload}, rest2}
  end
  def parse_will_message(packet, bin) do
    {packet, bin}
  end

  def parse_packet_id(<<packet_id::16, rest::binary>>), do: {packet_id, rest}

  def validate_packet_id(0), do: raise "bad packet id"
  def validate_packet_id(_), do: :ok

  def parse_properties(bin, ver) when ver !== Const.mqtt_proto_v5 do
    {nil, bin}
  end
  # TODO: version mess?
  def parse_properties(<<>>, Const.mqtt_proto_v5) do
    {%{}, <<>>}
  end
  def parse_properties(<<0, rest::binary>>, Const.mqtt_proto_v5) do
    {%{}, rest}
  end
  def parse_properties(bin, Const.mqtt_proto_v5) do
    {len, rest} = parse_variable_byte_integer(bin)
    <<props_bin::binary-size(len), rest1::binary>> = rest
    {parse_property(props_bin, %{}), rest1}
  end

  def parse_property(<<>>, props) do
    props
  end
  def parse_property(<<0x01, val, bin::binary>>, props) do
      parse_property(bin, Map.put(props, "Payload-Format-Indicator", val))
  end
  def parse_property(<<0x02, val::32, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Message-Expiry-Interval", val))
  end
  def parse_property(<<0x03, bin::binary>>, props) do
    {val, rest} = parse_utf8_string(bin)
    parse_property(rest, Map.put(props, "Content-Type", val))
  end
  def parse_property(<<0x08, bin::binary>>, props) do
      {val, rest} = parse_utf8_string(bin)
      parse_property(rest, Map.put(props, "Response-Topic", val))
  end
  def parse_property(<<0x09, len::16, val::binary-size(len), bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Correlation-Data", val))
  end
  def parse_property(<<0x0B, bin::binary>>, props) do
    {val, rest} = parse_variable_byte_integer(bin)
    parse_property(rest, Map.put(props, "Subscription-Identifier", val))
  end
  def parse_property(<<0x11, val::32, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Session-Expiry-Interval", val))
  end
  def parse_property(<<0x12, bin::binary>>, props) do
    {val, rest} = parse_utf8_string(bin)
    parse_property(rest, Map.put(props, "Assigned-Client-Identifier", val))
  end
  def parse_property(<<0x13, val::16, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Server-Keep-Alive", val))
  end
  def parse_property(<<0x15, bin::binary>>, props) do
    {val, rest} = parse_utf8_string(bin)
    parse_property(rest, Map.put(props, "Authentication-Method", val))
  end
  def parse_property(<<0x16, len::16, val::binary-size(len), bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Authentication-Data", val))
  end
  def parse_property(<<0x17, val, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Request-Problem-Information", val))
  end
  def parse_property(<<0x18, val::32, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Will-Delay-Interval", val))
  end
  def parse_property(<<0x19, val, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Request-Response-Information", val))
  end
  def parse_property(<<0x1A, bin::binary>>, props) do
    {val, rest} = parse_utf8_string(bin)
    parse_property(rest, Map.put(props, "Response-Information", val))
  end
  def parse_property(<<0x1C, bin::binary>>, props) do
    {val, rest} = parse_utf8_string(bin)
    parse_property(rest, Map.put(props, "Server-Reference", val))
  end
  def parse_property(<<0x1F, bin::binary>>, props) do
    {val, rest} = parse_utf8_string(bin)
    parse_property(rest, Map.put(props, "Reason-String", val))
  end
  def parse_property(<<0x21, val::16, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Receive-Maximum", val))
  end
  def parse_property(<<0x22, val::16, bin::binary>>, props) do
      parse_property(bin, Map.put(props, "Topic-Alias-Maximum", val))
  end
  def parse_property(<<0x23, val::16, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Topic-Alias", val))
  end
  def parse_property(<<0x24, val, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Maximum-QoS", val))
  end
  def parse_property(<<0x25, val, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Retain-Available", val))
  end
  def parse_property(<<0x26, bin::binary>>, props) do
      {pair, rest} = parse_utf8_pair(bin)
      parse_property(rest, Map.update(props, "User-Property", [pair], &(&1++[pair])))
  end
  def parse_property(<<0x27, val::32, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Maximum-Packet-Size", val))
  end
  def parse_property(<<0x28, val, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Wildcard-Subscription-Available", val))

  end
  def parse_property(<<0x29, val, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Subscription-Identifier-Available", val))
  end
  def parse_property(<<0x2A, val, bin::binary>>, props) do
    parse_property(bin, Map.put(props, "Shared-Subscription-Available", val))
  end

  def parse_variable_byte_integer(bin) do
    parse_variable_byte_integer(bin, 1, 0)
  end
  def parse_variable_byte_integer(<<1::1, len::7, rest::binary>>, multiplier, value) do
    parse_variable_byte_integer(rest, multiplier * Const.highbit, value + len * multiplier)
  end
  def parse_variable_byte_integer(<<0::1, len::7, rest::binary>>, multiplier, value) do
    {value + len * multiplier, rest}
  end

  def parse_topic_filters(:subscribe, bin) do
    for <<len::16, topic::binary-size(len), _::2, rh::2, rap::1, nl::1, qos::2>> <- bin do
      {topic, %{rh: rh, rap: rap, nl: nl, qos: validate_subqos(qos), rc: 0}}
    end
  end

  def parse_topic_filters(:unsubscribe, bin) do
    for <<len::16, topic::binary-size(len)>> <- bin do
      topic
    end
  end

  def parse_reason_codes(bin) do
    for <<code>> <- bin, do: code
  end

  def parse_utf8_pair(
    <<len1::16,
    key::binary-size(len1),
    len2::16,
    val::binary-size(len2),
    rest::binary>>) do
    {{key, val}, rest}
  end

  def parse_utf8_string(bin, false), do: {nil, bin}
  def parse_utf8_string(bin, true), do: parse_utf8_string(bin)
  def parse_utf8_string(<<len::16, str::binary-size(len), rest::binary>>), do: {str, rest}

  def parse_binary_data(<<len::16, data::binary-size(len), rest::binary>>) do
    {data, rest}
  end


  # -------------------------------------------------
  # Serialize MQTT Packet
  # -------------------------------------------------
  # @type serialize(%Packet.Mqtt{}) :: iodata()
  def serialize(%Packet.Mqtt{} = packet) do
    serialize(packet, Const.mqtt_proto_v4())
  end

  # @type serialize(%Packet.Mqtt{}, version()) :: iodata()
  def serialize(
        %Packet.Mqtt{
          header: header,
          variable: variable,
          payload: payload
        },
        ver) do
    serialize(header, serialize_variable(variable, ver), serialize_payload(payload))
  end

  # Serialize MQTT Header
  # @type serialize(%Packet.Header{}, binary(), binary()) :: iodata()
  def serialize(
        %Packet.Header{
          type: type,
          dup: dup,
          qos: qos,
          retain: retain
        },
        variable,
        payload) when Const.connect() <= type and type <= Const.auth() do
    len = :erlang.iolist_size(variable) + :erlang.iolist_size(payload)
    len <= Const.max_packet_size() or raise "mqtt frame too large"
    [
      <<type::4, flag(dup)::1, flag(qos)::2, flag(retain)::1>>,
      serialize_remaining_len(len),
      variable,
      payload
    ]
  end

  # Serialize MQTT Connect Packet
  def serialize_variable(
        %Packet.Connect{
          proto_name: proto_name,
          proto_ver: proto_ver,
          is_bridge: is_bridge,
          clean_start: clean_start,
          will_flag: will_flag,
          will_qos: will_qos,
          will_retain: will_retain,
          keepalive: keepalive,
          properties: properties,
          clientid: clientid,
          will_props: will_props,
          will_topic: will_topic,
          will_payload: will_payload,
          username: username,
          password: password
        },
        _ver) do
    [
      serialize_binary_data(proto_name),
      <<(case is_bridge do
          true -> 0x80 + proto_ver
          false -> proto_ver
        end)::8,
        (flag(username))::1,
        (flag(password))::1,
        (flag(will_retain))::1,
        will_qos::2,
        (flag(will_flag))::1,
        (flag(clean_start))::1,
        0::1,
        keepalive::unsigned-big-integer-size(16)>>,
      serialize_properties(properties, proto_ver),
      serialize_utf8_string(clientid),
      case will_flag do
        true ->
          [
            serialize_properties(will_props, proto_ver),
            serialize_utf8_string(will_topic),
            serialize_binary_data(will_payload)
          ]
        false ->
          <<>>
      end,
      serialize_utf8_string(username, true),
      serialize_utf8_string(password, true)
    ]
  end

  # Serialize MQTT Connack Packet
  def serialize_variable(
        %Packet.Connack{
          ack_flags: ack_flags,
          reason_code: reason_code,
          properties: properties
        },
        ver) do
    [ack_flags, reason_code, serialize_properties(properties, ver)]
  end

  # Serialize MQTT Publish Packet
  def serialize_variable(
        %Packet.Publish{
          topic_name: topic_name,
          packet_id: packet_id,
          properties: properties
        },
        ver) do
    [
      serialize_utf8_string(topic_name),
      if packet_id === nil do
        <<>>
      else
        <<packet_id::unsigned-big-integer-size(16)>>
      end,
      serialize_properties(properties, ver)
    ]
  end

  # Serialize MQTT Puback Packet
  def serialize_variable(
        %Packet.Puback{packet_id: packet_id},
        ver) when ver == Const.mqtt_proto_v3() or ver == Const.mqtt_proto_v4() do
    <<packet_id::unsigned-big-integer-size(16)>>
  end

  def serialize_variable(
        %Packet.Puback{
          packet_id: packet_id,
          reason_code: reason_code,
          properties: properties
        },
        Const.mqtt_proto_v5() = ver) do
    [
      <<packet_id::unsigned-big-integer-size(16)>>,
      reason_code,
      serialize_properties(properties, ver)
    ]
  end

  # Serialize MQTT Subscribe Packet
  def serialize_variable(
        %Packet.Subscribe{
          packet_id: packet_id,
          properties: properties,
          topic_filters: topic_filters
        },
        ver
      ) do
    [
      <<packet_id::unsigned-big-integer-size(16)>>,
      serialize_properties(properties, ver),
      serialize_topic_filters(:subscribe, topic_filters, ver)
    ]
  end

  # Serialize MQTT Unsubscribe Packet
  def serialize_variable(
        %Packet.Unsubscribe{
          packet_id: packet_id,
          properties: properties,
          topic_filters: topic_filters
        },
        ver) do
    [
      <<packet_id::unsigned-big-integer-size(16)>>,
      serialize_properties(properties, ver),
      serialize_topic_filters(:unsubscribe, topic_filters, ver)
    ]
  end

  # Serialize MQTT Suback Packet
  def serialize_variable(
        %Packet.Suback{
          packet_id: packet_id,
          properties: properties,
          reason_code: reason_code
        },
        ver) do
    [
      <<packet_id::unsigned-big-integer-size(16)>>,
      serialize_properties(properties, ver),
      serialize_reason_codes(reason_code)
    ]
  end

  # Serialize MQTT Unsuback Packet
  def serialize_variable(
        %Packet.Unsuback{
          packet_id: packet_id,
          properties: properties,
          reason_code: reason_code
        },
        ver) do
    [
      <<packet_id::unsigned-big-integer-size(16)>>,
      serialize_properties(properties, ver),
      serialize_reason_codes(reason_code)
    ]
  end

  # Serialize MQTT Disconnect Packet
  def serialize_variable(%Packet.Disconnect{}, ver)
      when ver == Const.mqtt_proto_v3() or ver == Const.mqtt_proto_v4() do
    <<>>
  end

  def serialize_variable(
        %Packet.Disconnect{
          reason_code: reason_code,
          properties: properties
        },
        ver = Const.mqtt_proto_v5()) do
    [reason_code, serialize_properties(properties, ver)]
  end

  def serialize_variable(%Packet.Disconnect{}, _ver) do
    <<>>
  end

  # Serialize MQTT Auth Packet
  def serialize_variable(
        %Packet.Auth{
          reason_code: reason_code,
          properties: properties
        },
        Const.mqtt_proto_v5() = ver) do
    [reason_code, serialize_properties(properties, ver)]
  end

  def serialize_variable(packet_id, Const.mqtt_proto_v3()) when is_integer(packet_id) do
    <<packet_id::unsigned-big-integer-size(16)>>
  end

  def serialize_variable(packet_id, Const.mqtt_proto_v4()) when is_integer(packet_id) do
    <<packet_id::unsigned-big-integer-size(16)>>
  end

  def serialize_variable(nil, _ver) do
    <<>>
  end

  def serialize_payload(nil), do: <<>>
  def serialize_payload(bin), do: bin

  def serialize_binary_data(bin) do
    [<<byte_size(bin)::unsigned-big-integer-size(16)>>, bin]
  end

  @spec serialize_properties(any, any) :: binary | [bitstring, ...]
  def serialize_properties(_props, ver) when ver !== Const.mqtt_proto_v5() do
    <<>>
  end

  def serialize_properties(props, Const.mqtt_proto_v5()) do
    serialize_properties(props)
  end

  def serialize_properties(nil) do
    <<0>>
  end

  def serialize_properties(props) when map_size(props) == 0 do
    <<0>>
  end

  def serialize_properties(props) when is_map(props) do
    bin =
      for {prop, val} <- props, into: <<>> do
        <<serialize_property(prop, val)::binary>>
      end

    [serialize_variable_byte_integer(byte_size(bin)), bin]
  end

  def serialize_property(_, nil) do
    <<>>
  end

  def serialize_property("Payload-Format-Indicator", val) do
    <<0x01, val>>
  end

  def serialize_property("Message-Expiry-Interval", val) do
    <<0x02, val::32>>
  end

  def serialize_property("Content-Type", val) do
    <<0x03, serialize_utf8_string(val)::binary>>
  end

  def serialize_property("Response-Topic", val) do
    <<0x08, serialize_utf8_string(val)::binary>>
  end

  def serialize_property("Correlation-Data", val) do
    <<0x09, byte_size(val)::16, val::binary>>
  end

  def serialize_property("Subscription-Identifier", val) do
    <<0x0B, serialize_variable_byte_integer(val)::binary>>
  end

  def serialize_property("Session-Expiry-Interval", val) do
    <<0x11, val::32>>
  end

  def serialize_property("Assigned-Client-Identifier", val) do
    <<0x12, serialize_utf8_string(val)::binary>>
  end

  def serialize_property("Server-Keep-Alive", val) do
    <<0x13, val::16>>
  end

  def serialize_property("Authentication-Method", val) do
    <<0x15, serialize_utf8_string(val)::binary>>
  end

  def serialize_property("Authentication-Data", val) do
    <<0x16, :erlang.iolist_size(val)::16, val::binary>>
  end

  def serialize_property("Request-Problem-Information", val) do
    <<0x17, val>>
  end

  def serialize_property("Will-Delay-Interval", val) do
    <<0x18, val::32>>
  end

  def serialize_property("Request-Response-Information", val) do
    <<0x19, val>>
  end

  def serialize_property("Response-Information", val) do
    <<0x1A, serialize_utf8_string(val)::binary>>
  end

  def serialize_property("Server-Reference", val) do
    <<0x1C, serialize_utf8_string(val)::binary>>
  end

  def serialize_property("Reason-String", val) do
    <<0x1F, serialize_utf8_string(val)::binary>>
  end

  def serialize_property("Receive-Maximum", val) do
    <<0x21, val::16>>
  end

  def serialize_property("Topic-Alias-Maximum", val) do
    <<0x22, val::16>>
  end

  def serialize_property("Topic-Alias", val) do
    <<0x23, val::16>>
  end

  def serialize_property("Maximum-QoS", val) do
    <<0x24, val>>
  end

  def serialize_property("Retain-Available", val) do
    <<0x25, val>>
  end

  def serialize_property("User-Property", {key, val}) do
    <<0x26, serialize_utf8_pair({key, val})::binary>>
  end

  def serialize_property("User-Property", props) when is_list(props) do
    for {key, value} <- props, into: <<>> do
      <<serialize_property("User-Property", {key, value})::binary>>
    end
  end

  def serialize_property("Maximum-Packet-Size", val) do
    <<0x27, val::32>>
  end

  def serialize_property("Wildcard-Subscription-Available", val) do
    <<0x28, val>>
  end

  def serialize_property("Subscription-Identifier-Available", val) do
    <<0x29, val>>
  end

  def serialize_property("Shared-Subscription-Available", val) do
    <<0x2A, val>>
  end

  def serialize_topic_filters(:subscribe, topic_filters, Const.mqtt_proto_v5()) do
    for {topic, %{rh: rh, rap: rap, nl: nl, qos: qos}} <- topic_filters, into: <<>> do
      <<serialize_utf8_string(topic)::binary, Const.reserved()::2, rh::2, flag(rap)::1,
        flag(nl)::1, qos::2>>
    end
  end

  def serialize_topic_filters(:subscribe, topic_filters, _ver) do
    for {topic, %{qos: qos}} <- topic_filters, into: <<>> do
      <<serialize_utf8_string(topic)::binary, Const.reserved()::6, qos::2>>
    end
  end

  def serialize_topic_filters(:unsubscribe, topic_filters, _ver) do
    for topic <- topic_filters, into: <<>> do
      <<serialize_utf8_string(topic)::binary>>
    end
  end

  def serialize_utf8_string(nil, false), do: raise("utf8 string undefined.")
  def serialize_utf8_string(nil, true), do: <<>>
  def serialize_utf8_string(string, _allow_null), do: serialize_utf8_string(string)

  def serialize_utf8_string(string) do
    len = byte_size(string)
    true = len <= 0xFFFF
    <<len::16, string::binary>>
  end

  def serialize_utf8_pair({name, value}) do
    <<serialize_utf8_string(name)::binary, serialize_utf8_string(value)::binary>>
  end

  def serialize_variable_byte_integer(n) when n <= Const.lowbits() do
    <<0::1, n::7>>
  end

  def serialize_variable_byte_integer(n) do
    <<1::1, rem(n, Const.highbit())::7,
      serialize_variable_byte_integer(div(n, Const.highbit()))::binary>>
  end

  def serialize_remaining_len(i) do
    serialize_variable_byte_integer(i)
  end

  def serialize_reason_codes(nil) do
    <<>>
  end

  def serialize_reason_codes(reason_codes) when is_list(reason_codes) do
    for code <- reason_codes, into: <<>> do
      <<code>>
    end
  end

  # -------------------------------------------------
  # Private Helper
  # -------------------------------------------------

  # Validate header if sctrict mode. See: mqtt-v5.0: 2.1.3 Flags
  def validate_header(Const.connect(), 0, 0, 0), do: :ok
  def validate_header(Const.connack(), 0, 0, 0), do: :ok
  def validate_header(Const.publish(), 0, Const.qos_0(), _), do: :ok
  def validate_header(Const.publish(), _, Const.qos_1(), _), do: :ok
  def validate_header(Const.publish(), 0, Const.qos_2(), _), do: :ok
  def validate_header(Const.puback(), 0, 0, 0), do: :ok
  def validate_header(Const.pubrec(), 0, 0, 0), do: :ok
  def validate_header(Const.pubrel(), 0, 1, 0), do: :ok
  def validate_header(Const.pubcomp(), 0, 0, 0), do: :ok
  def validate_header(Const.subscribe(), 0, 1, 0), do: :ok
  def validate_header(Const.suback(), 0, 0, 0), do: :ok
  def validate_header(Const.unsubscribe(), 0, 1, 0), do: :ok
  def validate_header(Const.unsuback(), 0, 0, 0), do: :ok
  def validate_header(Const.pingreq(), 0, 0, 0), do: :ok
  def validate_header(Const.pingresp(), 0, 0, 0), do: :ok
  def validate_header(Const.disconnect(), 0, 0, 0), do: :ok
  def validate_header(Const.auth(), 0, 0, 0), do: :ok
  def validate_header(_type, _dup, _qos, _rt), do: raise "bad frame header"

  defp none(opts), do: {:none, opts}
  defp merge_opts(options), do: Map.merge(@default_options, options)

  def validate_subqos(qos) when Const.qos_0 <= qos and qos <= Const.qos_2, do: qos
  def validate_subqos(_), do: raise "bad subqos"

  defp bool(0), do: false
  defp bool(1), do: true

  def mqtt_packet(header), do: %Packet.Mqtt{header: header}
  def mqtt_packet(header, variable), do: %Packet.Mqtt{header: header, variable: variable}
  def mqtt_packet(header, variable, payload), do: %Packet.Mqtt{header: header, variable: variable, payload: payload}

  defp flag(nil), do: Const.reserved()
  defp flag(false), do: 0
  defp flag(true), do: 1
  defp flag(x) when is_integer(x), do: x
  defp flag(b) when is_binary(b), do: 1

  defp fixqos(Const.pubrel(), 0), do: 1
  defp fixqos(Const.subscribe(), 0), do: 1
  defp fixqos(Const.unsubscribe(), 0), do: 1
  defp fixqos(_type, qos), do: qos
end
