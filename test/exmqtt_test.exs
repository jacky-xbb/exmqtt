defmodule ExmqttTest do
  use ExUnit.Case
  require Logger

  alias Exmqtt

  @moduletag capture_log: true

  @topics [<<"TopicA">>, <<"TopicA/B">>, <<"topic/c">>,
          <<"TopicA/c">>, <<"/TopicA">>]

  @wild_topics [<<"TopicA/+">>, <<"+/c">>, <<"#">>, <<"/#">>,
               <<"/+">>, <<"+/+">>, <<"TopicA/#">>]

  test "basic" do
    topic = Enum.at(@topics, 0)

    {:ok, c} = Exmqtt.start_link()
    {:ok, _} = Exmqtt.connect(c)
    {:ok, _, [1]} = Exmqtt.subscribe(c, topic, :qos1)
    {:ok, _, [2]} = Exmqtt.subscribe(c, topic, :qos2)
    {:ok, _} = Exmqtt.publish(c, topic, <<"qos 2">>, 2)
    {:ok, _} = Exmqtt.publish(c, topic, <<"qos 2">>, 2)
    {:ok, _} = Exmqtt.publish(c, topic, <<"qos 2">>, 2)
    assert 3 == length(receive_messages(3))
    :ok = Exmqtt.disconnect(c)
  end

  test "will message" do
    topic = Enum.at(@topics, 2)
    {:ok, c1} = Exmqtt.start_link([{:clean_start, true},
                                   {:will_topic, topic},
                                   {:will_payload, <<"client disconnected">>},
                                   {:keepalive, 2}])
    {:ok, _} = Exmqtt.connect(c1)

    {:ok, c2} = Exmqtt.start_link()
    {:ok, _} = Exmqtt.connect(c2)

    {:ok, _, [2]} = Exmqtt.subscribe(c2, topic, 2)
    :timer.sleep(10)
    :ok = Exmqtt.stop(c1)
    :timer.sleep(5)
    assert 1 == length(receive_messages(1))
    :ok = Exmqtt.disconnect(c2)
  end

  test "offline message queueing" do
    {:ok, c1} = Exmqtt.start_link([{:clean_start, false}, {:clientid, <<"c1">>}])
    {:ok, _} = Exmqtt.connect(c1)

    {:ok, _, [2]} = Exmqtt.subscribe(c1, Enum.at(@wild_topics, 5), 2)
    :ok = Exmqtt.disconnect(c1)
    {:ok, c2} = Exmqtt.start_link([{:clean_start, true}, {:clientid, <<"c2">>}])
    {:ok, _} = Exmqtt.connect(c2)

    :ok = Exmqtt.publish(c2, Enum.at(@topics, 1), <<"qos 0">>, 0)
    {:ok, _} = Exmqtt.publish(c2, Enum.at(@topics, 2), <<"qos 1">>, 1)
    {:ok, _} = Exmqtt.publish(c2, Enum.at(@topics, 3), <<"qos 2">>, 2)
    :timer.sleep(10)
    Exmqtt.disconnect(c2)
    {:ok, c3} = Exmqtt.start_link([{:clean_start, false}, {:clientid, <<"c1">>}])
    {:ok, _} = Exmqtt.connect(c3)

    :timer.sleep(10)
    Exmqtt.disconnect(c3)
    assert 3 == length(receive_messages(3))
  end

  test "overlapping subscriptions" do
    {:ok, c} = Exmqtt.start_link([])
    {:ok, _} = Exmqtt.connect(c)

    {:ok, _, [2, 1]} = Exmqtt.subscribe(c, [{Enum.at(@wild_topics, 6), 2}, {Enum.at(@wild_topics, 0), 1}])
    :timer.sleep(10)
    {:ok, _} = Exmqtt.publish(c, Enum.at(@topics, 3), <<"overlapping topic filters">>, 2)
    :timer.sleep(10)

    num = length(receive_messages(2))
    assert true == Enum.member?([1, 2], num)
    cond do
      num == 1 ->
        Logger.info("This server is publishing one message for all
                     matching overlapping subscriptions, not one for each.")
      num == 2 ->
        Logger.info("This server is publishing one message per each
                     matching overlapping subscription.")
      true -> :ok
    end
    Exmqtt.disconnect(c)
  end

  # test "redelivery on reconnect" do
  #   {:ok, c1} = Exmqtt.start_link([{:clean_start, false}, {:clientid, <<"c">>}])
  #   {:ok, _} = Exmqtt.connect(c1)

  #   {ok, _, [2]} = Exmqtt.subscribe(c1, Enum.at(@wild_topics, 6), 2)
  #   :timer.sleep(10)
  #   :ok = Exmqtt.pause(c1)
  #   {ok, _} = Exmqtt.publish(c1, Enum.at(@topics, 1), <<>>, [{:qos, 1}, {:retain, false}])
  #   {ok, _} = Exmqtt.publish(c1, Enum.at(@topics, 3), <<>>, [{:qos, 2}, {:retain, false}])
  #   :timer.sleep(10)
  #   :ok = Exmqtt.disconnect(c1)
  #   assert 0 == length(receive_messages(2))
  #   {:ok, c2} = Exmqtt.start_link([{:clean_start, false}, {:clientid, <<"c">>}])
  #   {:ok, _} = Exmqtt.connect(c2)

  #   :timer.sleep(10)
  #   :ok = Exmqtt.disconnect(c2)
  #   assert 2 == length(receive_messages(2))
  # end

  test "dollar topics" do
    {:ok, c} = Exmqtt.start_link([{:clean_start, true}, {:keepalive, 0}])
    {:ok, _} = Exmqtt.connect(c)

    {:ok, _, [1]} = Exmqtt.subscribe(c, Enum.at(@wild_topics, 5), 1)
    {:ok, _} = Exmqtt.publish(c, << <<"$">>::binary, (Enum.at(@topics, 1))::binary>>,
                              <<"test">>, [{:qos, 1}, {:retain, false}])
    :timer.sleep(10)
    assert 0 == length(receive_messages(1))
    :ok = Exmqtt.disconnect(c)
  end


  # -----------------------------------------------------------------
  # Helper
  # -----------------------------------------------------------------
  def receive_messages(count) do
    receive_messages(count, [])
  end

  def receive_messages(0, msgs) do
    msgs
  end
  def receive_messages(count, msgs) do
    receive do
        {:publish, msg} ->
          receive_messages(count-1, [msg | msgs])
        _other ->
          receive_messages(count, msgs)
    after 100 ->
        msgs
    end
  end
end
