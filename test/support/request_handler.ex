defmodule Exmqtt.Support.RequestHandler do
  require Logger

  alias Exmqtt
  alias Exmqtt.Frame
  alias Exmqtt.Packet

  @type qos() :: Exmqtt.qos_name() | Exmqtt.qos()
  @type topic() :: Exmqtt.topic()
  @type handler() :: ((corr_data :: binary(), req_payload :: binary()) -> rsp_payload :: binary())

  @spec start_link(topic(), qos(), handler(), Frame.options()) :: {:ok, pid()} | {:error, any()}
  def start_link(request_topic, qos, request_handler, options0) do
    parent = self()
    msg_handler = make_msg_handler(request_handler, parent)
    options = [{:msg_handler, msg_handler} | options0]
    case Exmqtt.start_link(options) do
        {:ok, pid} ->
          {:ok, _} = Exmqtt.connect(pid)
          try do
            case subscribe(pid, request_topic, qos) do
              :ok -> {:ok, pid}
              # {:error, _} = error -> error
            end
          rescue
            error_resaon ->
              Exmqtt.stop(pid)
              {:error, {error_resaon}}
          end
        {:error, _} = error -> error
    end
  end

  def stop(pid) do
    Exmqtt.disconnect(pid)
  end

  def make_msg_handler(request_handler, parent) do
    %{
      publish: fn(msg) -> handle_msg(msg, request_handler, parent) end,
      puback: fn(_ack) -> :ok end,
      disconnected: fn(_reason) -> :ok end
     }
  end

  def handle_msg(req_msg, request_handler, parent) do
    %{qos: qos, properties: props, payload: req_payload} = req_msg
    case Map.fetch(props, "Response-Topic") do
      {:ok, rsp_topic} when rsp_topic !== <<>> ->
        corr_data = Map.get(props, "Correlation-Data")
        rsp_props = :maps.without(["Response-Topic"], props)
        rsp_payload = request_handler.(corr_data, req_payload)
        rsp_msg = %Packet.Msg{
          qos: qos,
          topic: rsp_topic,
          props: rsp_props,
          payload: rsp_payload
        }
        Logger.debug("#{inspect(__MODULE__)} sending response msg to topic #{inspect(rsp_topic)}
                      with \n corr-data=#{inspect(corr_data)} \n payload=#{inspect(rsp_payload)}")
        :ok = send_response(rsp_msg)
      :error ->
        send(parent, {:discarded, req_payload})
        :ok
    end
  end

  def send_response(msg) do
    # This function is evaluated by emqtt itself.
    # hence delegate to another temp process for the loopback gen_statem call.
    client = self()
    _ = spawn_link(fn() ->
                    case Exmqtt.publish(client, msg) do
                        :ok -> :ok
                        {:ok, _} -> :ok
                        {:error, reason} -> exit({:failed_to_publish_response, reason})
                    end
                   end)
    :ok
  end

  def subscribe(client, topic, qos) do
    {:ok, _props, _qos} =
      Exmqtt.subscribe(client, [{topic, [{:rh, 2}, {:rap, false}, {:nl, true}, {:qos, qos}]}])
    :ok
  end
end
