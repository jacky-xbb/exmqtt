defmodule Exmqtt.RequestResponseTest do
  use ExUnit.Case
  require ExmqttConstants

  alias ExmqttConstants, as: Const
  alias Exmqtt.Support.RequestSender
  alias Exmqtt.Support.RequestHandler

  @moduletag capture_log: true

  setup do
    :emqx_ct_helpers.start_apps([])

    on_exit(fn ->
      :emqx_ct_helpers.stop_apps([])
    end)
  end

  test "request response" do
    request_response_per_qos(Const.qos_0)
    request_response_per_qos(Const.qos_1)
    request_response_per_qos(Const.qos_2)
  end

  def request_response_per_qos(qos) do
    req_topic = <<"request_topic">>
    rsp_topic = <<"response_topic">>
    {:ok, requester} = RequestSender.start_link(rsp_topic, qos,
                                                [{:proto_ver, :v5},
                                                {:clientid, <<"requester">>},
                                                {:properties, %{"Request-Response-Information" => 1}}])
    # This is a square service
    square = fn(_corr_data, req_bin) ->
      i = b2i(req_bin)
      i2b(i * i)
    end
    {:ok, responser} = RequestHandler.start_link(req_topic, qos, square,
                                                 [{:proto_ver, :v5},
                                                 {:clientid, <<"responser">>}])
    :ok = RequestSender.send(requester, req_topic, rsp_topic, <<"corr-1">>, <<"2">>, qos)
    receive do
      {:response, <<"corr-1">>, <<"4">>} ->
        :ok
      other ->
        :erlang.error({:unexpected, other})
    after
      100 ->
        :erlang.error(:timeout)
    end

    :ok = RequestSender.stop(requester)
    :ok = RequestHandler.stop(responser)
  end

  def b2i(b), do: :erlang.binary_to_integer(b)
  def i2b(i), do: :erlang.integer_to_binary(i)

end
