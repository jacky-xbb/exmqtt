defmodule Exmqtt.Support.RequestSender do
  alias Exmqtt
  alias Exmqtt.Packet

  def start_link(response_topic, qos, options0) do
    parent = self()
    msg_handler = make_msg_handler(parent)
    options = [{:msg_handler, msg_handler} | options0]
    case Exmqtt.start_link(options) do
      {:ok, pid} ->
        {:ok, _} = Exmqtt.connect(pid)
        try do
          case subscribe(pid, response_topic, qos) do
            :ok ->
              {:ok, pid}
            {:error, _} = error ->
              error
          end
        rescue
          error_reason ->
            Exmqtt.stop(pid)
            {:error, {error_reason}}
        end
      {:error, _} = error -> error
    end

  end

  # Send a message to request topic with correlation-data `corr_data'.
  # Response should be delivered as a `{response, corr_data, payload}'
  def send(client, req_topic, rsp_topic, corr_data, payload, qos) do
    props = %{
      "Response-Topic" => rsp_topic,
      "Correlation-Data" => corr_data
    }
    msg = %Packet.Msg{
      qos: qos,
      topic: req_topic,
      props: props,
      payload: payload
    }
    case Exmqtt.publish(client, msg) do
      :ok -> :ok       # qos = 0
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  def stop(pid) do
    Exmqtt.disconnect(pid)
  end

  def subscribe(client, topic, qos) do
    case Exmqtt.subscribe(client, topic, qos) do
      {:ok, _, _} -> :ok
      {:error, _} = error -> error
    end
  end

  def make_msg_handler(parent) do
    %{
      publish: fn(msg) -> handle_msg(msg, parent) end,
      puback: fn(_ack) -> :ok end,
      disconnected: fn(_reason) -> :ok end
     }
  end

  def handle_msg(msg, parent) do
    %{properties: props, payload: payload} = msg
    corr_data = Map.get(props, "Correlation-Data")
    send(parent, {:response, corr_data, payload})
    :ok
  end
end
