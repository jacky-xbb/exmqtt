defmodule ExmqttTest do
  use ExUnit.Case
  require Logger

  alias Exmqtt

  @topics [<<"TopicA">>, <<"TopicA/B">>, <<"Topic/C">>,
          <<"TopicA/C">>, <<"/TopicA">>]

  @wild_topics [<<"TopicA/+">>, <<"+/C">>, <<"#">>, <<"/#">>,
               <<"/+">>, <<"+/+">>, <<"TopicA/#">>]

  test "basic test" do
    topic = Enum.at(@topics, 0)
    Logger.info("Basic test starting")

    {:ok, c} = Exmqtt.start_link()
    {:ok, _} = Exmqtt.connect(c)
    # {:ok, _, [1]} = emqtt:subscribe(C, Topic, qos1)
    # {:ok, _, [2]} = emqtt:subscribe(C, Topic, qos2),
    # {:ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    # {:ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    # {:ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    # ?assertEqual(3, length(receive_messages(3))),
    :ok = emqtt:disconnect(c)
  end
end
