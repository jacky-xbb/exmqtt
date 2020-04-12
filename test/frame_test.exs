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
      assert {:ok, packet, <<>>, _} = Frame.parse(bin)
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
      assert {ok, packet, <<>>, _} = Frame.parse(bin)
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

end
