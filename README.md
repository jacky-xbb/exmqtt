exmqtt
=====

Elixir MQTT v5.0 Client compatible with MQTT v3.0

Here is the simple usage of `Exmqtt` library.

``` elixir
client_id = <<"test">>
{:ok, conn_pid} = Exmqtt.start_link([{:clientid, client_id}])
{:ok, _props} = Exmqtt.connect(conn_pid)
topic = <<"guide/#">>
qos = 1
{:ok, _props, _reason_code} = Exmqtt.subscribe(conn_pid, {topic, qos})
{:ok, _pkt_id} = Exmqtt.publish(conn_pid, <<"guide/1">>, <<"Hello World!">>, qos)
# If the qos of publish packet is 0, `publish` function would not return packetid
:ok = Exmqtt.publish(conn_pid, <<"guide/2">>, <<"Hello World!">>, 0)

# Recursively get messages in elixir shell.
rec = fn rec ->
  receive do
    {:publish, msg} ->
      IO.puts("Msg: #{inspect(msg)}")
      rec.(rec)
    _other -> rec.(rec)
  after 5 ->
    :ok
  end
end

rec.(rec)

{:ok, _props, _reason_code} = Exmqtt.unsubscribe(conn_pid, <<"guide/#">>)

:ok = Exmqtt.disconnect(conn_pid)
```

Not only the `clientid` can be passed as parameter, but also a lot of other options
 can be passed as parameters.

Here is the options which could be passed into Exmqtt.start_link/1.

``` elixir
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
```

## Installation

Exmqtt can be installed by adding `exmqtt` to your list of
dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:exmqtt, "~> 0.1.0"}
  ]
end
```

## Testing

**Note**: Start emqx locally firstly

```shell
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx
```

Run tests
```elixir
mix test
```


## License

Apache License Version 2.0

## Author

yummy.bian@gmail.com
