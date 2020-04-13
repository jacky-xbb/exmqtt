defmodule Exmatt.FrameTest do
  use ExUnit.Case
  require ExmqttConstants
  alias ExmqttConstants, as: Const
  alias Exmqtt.Frame
  alias Exmqtt.Packet

  describe "connect packet" do
    test "serialize and parse the connection packet" do
      packet1 = Packet.connect_packet(%Packet.Connect{})
      assert packet1 == parse_serialize(packet1)
      packet2 = Packet.connect_packet(%Packet.Connect{
        clientid: <<"clientId">>,
        will_qos: Const.qos_1,
        will_flag: true,
        will_retain: true,
        will_topic: <<"will">>,
        will_payload: <<"bye">>,
        clean_start: true
      })
      assert packet2 == parse_serialize(packet2)
    end

    test "parse the connection packet of v3" do
      bin = <<16,37,0,6,77,81,73,115,100,112,3,2,0,60,0,23,109,111,115,
              113,112,117, 98,47,49,48,52,53,49,45,105,77,97,99,46,108,
              111,99,97>>
      packet = Packet.connect_packet(
        %Packet.Connect{
          proto_ver: Const.mqtt_proto_v3,
          proto_name: <<"MQIsdp">>,
          clientid: <<"mosqpub/10451-iMac.loca">>,
          clean_start: true,
          keepalive: 60
        })
      assert {:ok, ^packet, <<>>, _} = Frame.parse(bin)
    end

    test "serialize and parse the connection packet of v4" do
      bin = <<16,35,0,4,77,81,84,84,4,2,0,60,0,23,109,111,115,113,112,117,
            98,47,49,48,52,53,49,45,105,77,97,99,46,108,111,99,97>>
      packet = Packet.connect_packet(%Packet.Connect{proto_ver: 4,
                                                    proto_name: <<"MQTT">>,
                                                    clientid: <<"mosqpub/10451-iMac.loca">>,
                                                    clean_start: true,
                                                    keepalive: 60})
      assert bin == serialize_to_binary(packet)
      assert {ok, ^packet, <<>>, _} = Frame.parse(bin)
    end

    test "serialize and parse the connection packet of v5" do
      props = %{
        "Session-Expiry-Interval"      => 60,
        "Receive-Maximum"              => 100,
        "Maximum-QoS"                  => Const.qos_2,
        "Retain-Available"             => 1,
        "Maximum-Packet-Size"          => 1024,
        "Topic-Alias-Maximum"          => 10,
        "Request-Response-Information" => 1,
        "Request-Problem-Information"  => 1,
        "Authentication-Method"        => <<"oauth2">>,
        "Authentication-Data"          => <<"33kx93k">>
      }

      will_props = %{
        "Will-Delay-Interval"      => 60,
        "Payload-Format-Indicator" => 1,
        "Message-Expiry-Interval"  => 60,
        "Content-Type"             => <<"text/json">>,
        "Response-Topic"           => <<"topic">>,
        "Correlation-Data"         => <<"correlateid">>,
        "User-Property"            => [{<<"k">>, <<"v">>}]
      }
      packet = Packet.connect_packet(
        %Packet.Connect{
          proto_name: <<"MQTT">>,
          proto_ver: Const.mqtt_proto_v5,
          is_bridge: false,
          clean_start: true,
          clientid: <<>>,
          will_flag: true,
          will_qos: Const.qos_1,
          will_retain: false,
          keepalive: 60,
          properties: props,
          will_props: will_props,
          will_topic: <<"topic">>,
          will_payload: <<>>,
          username: <<"device:1">>,
          password: <<"passwd">>
       })
       assert packet == parse_serialize(packet)
    end

    test "serialize and parse the connection packet without client ID" do
        bin = <<16,12,0,4,77,81,84,84,4,2,0,60,0,0>>
        packet = Packet.connect_packet(
          %Packet.Connect{
            proto_ver: 4,
            proto_name: <<"MQTT">>,
            clientid: <<>>,
            clean_start: true,
            keepalive: 60
          })
        assert bin == serialize_to_binary(packet)
        assert {ok, ^packet, <<>>, _} = Frame.parse(bin)
    end

    test "serialize and parse the connection packet with will" do
      bin = <<16,67,0,6,77,81,73,115,100,112,3,206,0,60,0,23,109,111,115,113,112,
              117,98,47,49,48,52,53,50,45,105,77,97,99,46,108,111,99,97,0,5,47,119,
              105,108,108,0,7,119,105,108,108,109,115,103,0,4,116,101,115,116,0,6,
              112,117,98,108,105,99>>
      packet = %Packet.Mqtt{
        header: %Packet.Header{type: Const.connect},
        variable: %Packet.Connect{
          proto_ver: Const.mqtt_proto_v3,
          proto_name: <<"MQIsdp">>,
          clientid: <<"mosqpub/10452-iMac.loca">>,
          clean_start: true,
          keepalive: 60,
          will_retain: false,
          will_qos: Const.qos_1,
          will_flag: true,
          will_topic: <<"/will">>,
          will_payload: <<"willmsg">>,
          username: <<"test">>,
          password: <<"public">>
        }
      }
      assert bin == serialize_to_binary(packet)
      assert {ok, ^packet, <<>>, _} = Frame.parse(bin)
    end

    test "serialize and parse the bridge connection packet" do
      bin = <<16,86,0,6,77,81,73,115,100,112,131,44,0,60,0,19,67,95,48,48,58,48,67,
              58,50,57,58,50,66,58,55,55,58,53,50,0,48,36,83,89,83,47,98,114,111,107,
              101,114,47,99,111,110,110,101,99,116,105,111,110,47,67,95,48,48,58,48,
              67,58,50,57,58,50,66,58,55,55,58,53,50,47,115,116,97,116,101,0,1,48>>
      topic = <<"$SYS/broker/connection/C_00:0C:29:2B:77:52/state">>
      packet = %Packet.Mqtt{
        header: %Packet.Header{type: Const.connect},
        variable: %Packet.Connect{
          clientid: <<"C_00:0C:29:2B:77:52">>,
          proto_ver: 0x03,
          proto_name: <<"MQIsdp">>,
          is_bridge: true,
          will_retain: true,
          will_qos: Const.qos_1,
          will_flag: true,
          clean_start: false,
          keepalive: 60,
          will_topic: topic,
          will_payload: <<"0">>
      }}
      assert bin == serialize_to_binary(packet)
      assert {ok, ^packet, <<>>, _} = Frame.parse(bin)
    end
  end

  describe "connack packet" do
    test "serialize and parse the connack packet" do
      packet = Packet.connack_packet(Const.rc_success)
      assert <<32,2,0,0>> == serialize_to_binary(packet)
      assert packet == parse_serialize(packet)
    end

    test "serialize and parse the connack packet of v5" do
      props = %{
        "Session-Expiry-Interval"            => 60,
        "Receive-Maximum"                    => 100,
        "Maximum-QoS"                        => Const.qos_2,
        "Retain-Available"                   => 1,
        "Maximum-Packet-Size"                => 1024,
        "Assigned-Client-Identifier"         => <<"id">>,
        "Topic-Alias-Maximum"                => 10,
        "Reason-String"                      => <<>>,
        "Wildcard-Subscription-Available"    => 1,
        "Subscription-Identifier-Available"  => 1,
        "Shared-Subscription-Available"      => 1,
        "Server-Keep-Alive"                  => 60,
        "Response-Information"               => <<"response">>,
        "Server-Reference"                   => <<"192.168.1.10">>,
        "Authentication-Method"              => <<"oauth2">>,
        "Authentication-Data"                => <<"33kx93k">>
      }
      packet = Packet.connack_packet(Const.rc_success, 0, props)
      assert packet == parse_serialize(packet, %{version: Const.mqtt_proto_v5})
    end
  end

  describe "publish packet" do
    test "serialize and parse the qos0 publish packet" do
      bin = <<48,14,0,7,120,120,120,47,121,121,121,104,101,108,108,111>>
      packet = %Packet.Mqtt{
        header: %Packet.Header{
          type: Const.publish,
          dup: false,
          qos: Const.qos_0,
          retain: false
        },
        variable: %Packet.Publish{
          topic_name: <<"xxx/yyy">>,
          packet_id: nil
        },
        payload: <<"hello">>
      }
      assert bin == serialize_to_binary(packet)
      assert {ok, ^packet, <<>>, _} = Frame.parse(bin)
    end

    test "serialize and parse the qos1 publish packet" do
        bin = <<50,13,0,5,97,47,98,47,99,0,1,104,97,104,97>>
        packet = %Packet.Mqtt{
          header: %Packet.Header{
            type: Const.publish,
            dup: false,
            qos: Const.qos_1,
            retain: false
          },
          variable: %Packet.Publish{
            topic_name: <<"a/b/c">>,
            packet_id: 1
          },
          payload: <<"haha">>
        }
        assert bin == serialize_to_binary(packet)
        assert {ok, ^packet, <<>>, _} = Frame.parse(bin)
    end

    test "serialize and parse the qos2 publish packet" do
      packet = Packet.publish_packet(Const.qos_2, <<"Topic">>, 1, payload())
      assert packet == parse_serialize(packet)
    end

    test "serialize and parse the publish packet of v5" do
      props = %{
        "Payload-Format-Indicator" => 1,
        "Message-Expiry-Interval"  => 60,
        "Topic-Alias"              => 0xAB,
        "Response-Topic"           => <<"reply">>,
        "Correlation-Data"         => <<"correlation-id">>,
        "Subscription-Identifier"  => 1,
        "Content-Type"             => <<"text/json">>
      }
      packet = Packet.publish_packet(Const.qos_1, <<"$share/group/topic">>, 1, props, <<"payload">>)
      assert packet == parse_serialize(packet, %{version: Const.mqtt_proto_v5})
    end
  end

  describe "puback packet" do
    test "serialize and parse the puback packet" do
      packet = Packet.puback_packet(1)
      assert <<64,2,0,1>> == serialize_to_binary(packet)
      assert packet == parse_serialize(packet)
    end

    test "serialize and parse the puback packet of v5" do
      packet = Packet.puback_packet(16, Const.rc_success, %{"Reason-String" => <<"success">>})
      assert packet == parse_serialize(packet, %{version: Const.mqtt_proto_v5})
    end

    test "serialize and parse the pubrec packet" do
      packet = Packet.pubrec_packet(1)
      assert <<5::4, 0::4, 2, 0, 1>> == serialize_to_binary(packet)
      assert packet == parse_serialize(packet)
    end

    test "serialize and parse the pubrec packet of v5" do
      packet = Packet.pubrec_packet(16, Const.rc_success, %{"Reason-String" => <<"success">>})
      assert packet == parse_serialize(packet, %{version: Const.mqtt_proto_v5})
    end

    test "serialize and parse the pubrel packet" do
      packet = Packet.pubrel_packet(1)
      bin = serialize_to_binary(packet)
      assert <<6::4, 2::4, 2, 0, 1>> == bin
      assert packet == parse_serialize(packet)
    end

    test "serialize and parse the pubrel packet of v5" do
      packet = Packet.pubrel_packet(16, Const.rc_success, %{"Reason-String" => <<"success">>})
      assert packet == parse_serialize(packet, %{version: Const.mqtt_proto_v5})
    end

    test "serialize and parse the pubcomp packet" do
      packet = Packet.pubcomp_packet(1)
      bin = serialize_to_binary(packet)
      assert <<7::4, 0::4, 2, 0, 1>> == bin
      assert packet == parse_serialize(packet)
    end

    test "serialize and parse the pubcomp packet of v5" do
      packet = Packet.pubcomp_packet(16, Const.rc_success, %{"Reason-String" => <<"success">>})
      assert packet == parse_serialize(packet, %{version: Const.mqtt_proto_v5})
    end
  end

  describe "subscribe packet" do
    test "serialize and parse the subscribe packet" do
      bin = <<130,11,0,2,0,6,84,111,112,105,99,65,2>>
      topic_opts = %{nl: 0 , rap: 0, rc: 0, rh: 0, qos: 2}
      topic_filters = [{<<"TopicA">>, topic_opts}]
      packet = Packet.subscribe_packet(2, topic_filters)
      assert bin == serialize_to_binary(packet)
      assert {ok, ^packet, <<>>, _} = Frame.parse(bin)
    end
  end


  # --------------------------
  # Helper
  # --------------------------
  defp parse_serialize(packet) do
    parse_serialize(packet, %{version: Const.mqtt_proto_v4})
  end

  defp parse_serialize(packet, %{version: ver} = opts) when is_map(opts) do
    bin = :erlang.iolist_to_binary(Frame.serialize(packet, ver))
    parse_state = Frame.initial_parse_state(opts)
    {:ok, n_packet, <<>>, _} = Frame.parse(bin, parse_state)
    n_packet
  end

  def serialize_to_binary(packet) do
    :erlang.iolist_to_binary(Frame.serialize(packet))
  end

  defp payload() do
    1..1000
    |> Enum.map(fn _x -> "payload." end)
    |> :erlang.iolist_to_binary()
  end

end
