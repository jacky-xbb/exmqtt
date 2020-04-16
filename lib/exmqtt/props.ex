defmodule Exmqtt.Props do
  require ExmqttConstants
  alias ExmqttConstants, as: Const
  alias Exmqtt.Errors.BadProperty

  @props_table %{
    0x01 => {"Payload-Format-Indicator", "Byte", [Const.publish]},
    0x02 => {"Message-Expiry-Interval", "Four-Byte-Integer", [Const.publish]},
    0x03 => {"Content-Type", "UTF8-Encoded-String", [Const.publish]},
    0x08 => {"Response-Topic", "UTF8-Encoded-String", [Const.publish]},
    0x09 => {"Correlation-Data", "Binary-Data", [Const.publish]},
    0x0B => {"Subscription-Identifier", "Variable-Byte-Integer", [Const.publish, Const.subscribe]},
    0x11 => {"Session-Expiry-Interval", "Four-Byte-Integer", [Const.connect, Const.connack, Const.disconnect]},
    0x12 => {"Assigned-Client-Identifier", "UTF8-Encoded-String", [Const.connack]},
    0x13 => {"Server-Keep-Alive", "Two-Byte-Integer", [Const.connack]},
    0x15 => {"Authentication-Method", "UTF8-Encoded-String", [Const.connect, Const.connack, Const.auth]},
    0x16 => {"Authentication-Data", "Binary-Data", [Const.connect, Const.connack, Const.auth]},
    0x17 => {"Request-Problem-Information", "Byte", [Const.connect]},
    0x18 => {"Will-Delay-Interval", "Four-Byte-Integer", ["WILL"]},
    0x19 => {"Request-Response-Information", "Byte", [Const.connect]},
    0x1A => {"Response-Information", "UTF8-Encoded-String", [Const.connack]},
    0x1C => {"Server-Reference", "UTF8-Encoded-String", [Const.connack, Const.disconnect]},
    0x1F => {"Reason-String", "UTF8-Encoded-String", "ALL"},
    0x21 => {"Receive-Maximum", "Two-Byte-Integer", [Const.connect, Const.connack]},
    0x22 => {"Topic-Alias-Maximum", "Two-Byte-Integer", [Const.connect, Const.connack]},
    0x23 => {"Topic-Alias", "Two-Byte-Integer", [Const.publish]},
    0x24 => {"Maximum-QoS", "Byte", [Const.connack]},
    0x25 => {"Retain-Available", "Byte", [Const.connack]},
    0x26 => {"User-Property", "UTF8-String-Pair", "ALL"},
    0x27 => {"Maximum-Packet-Size", "Four-Byte-Integer", [Const.connect, Const.connack]},
    0x28 => {"Wildcard-Subscription-Available", "Byte", [Const.connack]},
    0x29 => {"Subscription-Identifier-Available", "Byte", [Const.connack]},
    0x2A => {"Shared-Subscription-Available", "Byte", [Const.connack]}
  }

  def id(key) do
    case key do
      "Payload-Format-Indicator"          -> 0x01
      "Message-Expiry-Interval"           -> 0x02
      "Content-Type"                      -> 0x03
      "Response-Topic"                    -> 0x08
      "Correlation-Data"                  -> 0x09
      "Subscription-Identifier"           -> 0x0B
      "Session-Expiry-Interval"           -> 0x11
      "Assigned-Client-Identifier"        -> 0x12
      "Server-Keep-Alive"                 -> 0x13
      "Authentication-Method"             -> 0x15
      "Authentication-Data"               -> 0x16
      "Request-Problem-Information"       -> 0x17
      "Will-Delay-Interval"               -> 0x18
      "Request-Response-Information"      -> 0x19
      "Response-Information"              -> 0x1A
      "Server-Reference"                  -> 0x1C
      "Reason-String"                     -> 0x1F
      "Receive-Maximum"                   -> 0x21
      "Topic-Alias-Maximum"               -> 0x22
      "Topic-Alias"                       -> 0x23
      "Maximum-QoS"                       -> 0x24
      "Retain-Available"                  -> 0x25
      "User-Property"                     -> 0x26
      "Maximum-Packet-Size"               -> 0x27
      "Wildcard-Subscription-Available"   -> 0x28
      "Subscription-Identifier-Available" -> 0x29
      "Shared-Subscription-Available"     -> 0x2A
    end
  end

  def filter(packet_type, props)
    when Const.connect <= packet_type and packet_type <= Const.auth do
      props
      |> Enum.filter(fn {name, _} ->
        case Map.fetch(@props_table, id(name)) do
          {:ok, {_name, _type, "All"}} ->
            true
          {:ok, {_name, _type, allowed_types}} ->
            Enum.member?(packet_type, allowed_types)
          :error ->
            false
        end
      end)
  end

  def vilidate(props) when is_map(props) do
    props |> Enum.each(&validate_prop/1)
  end

  def validate_prop({name, val} = prop) do
    case Map.fetch(@props_table, id(name)) do
      {:ok, {_name, type, _}}  ->
        validate_value(type, val) or raise BadProperty, prop
      :error ->
        raise BadProperty, prop
    end
  end

  # Helper
  defp validate_value("Byte", val), do: is_integer(val)
  defp validate_value("Two-Byte-Integer", val), do: is_integer(val)
  defp validate_value("Four-Byte-Integer", val), do: is_integer(val)
  defp validate_value("Variable-Byte-Integer", val), do: is_integer(val)
  defp validate_value("UTF8-Encoded-String", val), do: is_binary(val)
  defp validate_value("Binary-Data", val), do: is_binary(val)
  defp validate_value("UTF8-String-Pair", val), do: is_tuple(val) or is_list(val)
end
