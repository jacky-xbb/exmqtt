defmodule Exmqtt.Packet do
  require ExmqttConstants
  alias ExmqttConstants, as: Const

  defmodule Mqtt do
    defstruct [:header, :variable, :payload]
  end

  defmodule Header do
    defstruct type: Const.reserved(),
              dup: false,
              qos: Const.qos_0(),
              retain: false
  end

  # ----------------------------
  # MQTT Message
  # ----------------------------
  defmodule Msg do
    defstruct packet_id: nil,
              topic: nil,
              props: nil,
              payload: nil,
              qos: Const.qos_0,
              retain: false,
              dup: false
  end

  # ----------------------------
  # MQTT Packet
  # ----------------------------
  defmodule Connect do
    defstruct proto_name: "MQTT",
              proto_ver: Const.mqtt_proto_v4(),
              is_bridge: false,
              clean_start: true,
              will_flag: false,
              will_qos: Const.qos_0(),
              will_retain: false,
              keepalive: 0,
              properties: nil,
              clientid: <<>>,
              will_props: nil,
              will_topic: nil,
              will_payload: nil,
              username: nil,
              password: nil
  end

  defmodule Connack do
    defstruct [:ack_flags, :reason_code, :properties]
  end

  defmodule Publish do
    defstruct [:topic_name, :packet_id, :properties]
  end

  defmodule Puback do
    defstruct [:packet_id, :reason_code, :properties]
  end

  defmodule Subscribe do
    defstruct [:packet_id, :properties, :topic_filters]
  end

  defmodule Suback do
    defstruct [:packet_id, :properties, :reason_code]
  end

  defmodule Unsubscribe do
    defstruct [:packet_id, :properties, :topic_filters]
  end

  defmodule Unsuback do
    defstruct [:packet_id, :properties, :reason_code]
  end

  defmodule Disconnect do
    defstruct [:reason_code, :properties]
  end

  defmodule Auth do
    defstruct [:reason_code, :properties]
  end

  # ----------------------------
  # MQTT Packet Match
  # ----------------------------
  @spec connect_packet(any) :: Exmqtt.Package.Mqtt.t()
  def connect_packet(var) do
    %Mqtt{
      header: %Header{type: Const.connect()},
      variable: var
    }
  end

  @spec connack_packet(any) :: Exmqtt.Package.Mqtt.t()
  def connack_packet(reason_code) do
    %Mqtt{
      header: %Header{type: Const.connack()},
      variable: %Connack{
        ack_flags: 0,
        reason_code: reason_code
      }
    }
  end

  @spec connack_packet(any, any) :: Exmqtt.Package.Mqtt.t()
  def connack_packet(reason_code, sess_present) do
    %Mqtt{
      header: %Header{type: Const.connack()},
      variable: %Connack{
        ack_flags: sess_present,
        reason_code: reason_code
      }
    }
  end

  @spec connack_packet(any, any, any) :: Exmqtt.Package.Mqtt.t()
  def connack_packet(reason_code, sess_present, properties) do
    %Mqtt{
      header: %Header{type: Const.connack()},
      variable: %Connack{
        ack_flags: sess_present,
        reason_code: reason_code,
        properties: properties
      }
    }
  end

  @spec auth_packet :: Exmqtt.Packet.Mqtt.t()
  def auth_packet() do
    %Mqtt{
      header: %Header{type: Const.auth()},
      variable: %Auth{
        reason_code: 0
      }
    }
  end

  @spec auth_packet(any) :: Exmqtt.Packet.Mqtt.t()
  def auth_packet(reason_code) do
    %Mqtt{
      header: %Header{type: Const.auth()},
      variable: %Auth{
        reason_code: reason_code
      }
    }
  end

  @spec auth_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def auth_packet(reason_code, properties) do
    %Mqtt{
      header: %Header{type: Const.auth()},
      variable: %Auth{
        reason_code: reason_code,
        properties: properties
      }
    }
  end

  @spec publish_packet(any) :: Exmqtt.Packet.Mqtt.t()
  def publish_packet(qos) do
    %Mqtt{
      header: %Header{
        type: Const.publish(),
        qos: qos
      }
    }
  end

  @spec publish_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def publish_packet(qos, packet_id) do
    %Mqtt{
      header: %Header{
        type: Const.publish(),
        qos: qos
      },
      variable: %Publish{packet_id: packet_id}
    }
  end

  @spec publish_packet(any, any, any, any) :: Exmqtt.Packet.Mqtt.t()
  def publish_packet(qos, topic, packet_id, payload) do
    %Mqtt{
      header: %Header{
        type: Const.publish(),
        qos: qos
      },
      variable: %Publish{
        topic_name: topic,
        packet_id: packet_id
      },
      payload: payload
    }
  end

  @spec publish_packet(any, any, any, any, any) :: Exmqtt.Packet.Mqtt.t()
  def publish_packet(qos, topic, packet_id, properties, payload) do
    %Mqtt{
      header: %Header{
        type: Const.publish(),
        qos: qos
      },
      variable: %Publish{
        topic_name: topic,
        packet_id: packet_id,
        properties: properties
      },
      payload: payload
    }
  end

  @spec puback_packet(any) :: Exmqtt.Packet.Mqtt.t()
  def puback_packet(packet_id) do
    %Mqtt{
      header: %Header{type: Const.puback()},
      variable: %Puback{
        packet_id: packet_id,
        reason_code: 0
      }
    }
  end

  @spec puback_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def puback_packet(packet_id, reason_code) do
    %Mqtt{
      header: %Header{type: Const.puback()},
      variable: %Puback{
        packet_id: packet_id,
        reason_code: reason_code
      }
    }
  end

  @spec puback_packet(any, any, any) :: Exmqtt.Packet.Mqtt.t()
  def puback_packet(packet_id, reason_code, properties) do
    %Mqtt{
      header: %Header{type: Const.puback()},
      variable: %Puback{
        packet_id: packet_id,
        reason_code: reason_code,
        properties: properties
      }
    }
  end

  @spec pubrec_packet(any) :: Exmqtt.Packet.Mqtt.t()
  def pubrec_packet(packet_id) do
    %Mqtt{
      header: %Header{type: Const.pubrec()},
      variable: %Puback{
        packet_id: packet_id,
        reason_code: 0
      }
    }
  end

  @spec pubrec_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def pubrec_packet(packet_id, reason_code) do
    %Mqtt{
      header: %Header{type: Const.pubrec()},
      variable: %Puback{
        packet_id: packet_id,
        reason_code: reason_code
      }
    }
  end

  @spec pubrec_packet(any, any, any) :: Exmqtt.Packet.Mqtt.t()
  def pubrec_packet(packet_id, reason_code, properties) do
    %Mqtt{
      header: %Header{type: Const.pubrec()},
      variable: %Puback{
        packet_id: packet_id,
        reason_code: reason_code,
        properties: properties
      }
    }
  end

  @spec pubrel_packet(any) :: Exmqtt.Packet.Mqtt.t()
  def pubrel_packet(packet_id) do
    %Mqtt{
      header: %Header{
        type: Const.pubrel(),
        qos: Const.qos_1()
      },
      variable: %Puback{
        packet_id: packet_id,
        reason_code: 0
      }
    }
  end

  @spec pubrel_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def pubrel_packet(packet_id, reason_code) do
    %Mqtt{
      header: %Header{
        type: Const.pubrel(),
        qos: Const.qos_1()
      },
      variable: %Puback{
        packet_id: packet_id,
        reason_code: reason_code
      }
    }
  end

  @spec pubrel_packet(any, any, any) :: Exmqtt.Packet.Mqtt.t()
  def pubrel_packet(packet_id, reason_code, properties) do
    %Mqtt{
      header: %Header{
        type: Const.pubrel(),
        qos: Const.qos_1()
      },
      variable: %Puback{
        packet_id: packet_id,
        reason_code: reason_code,
        properties: properties
      }
    }
  end

  @spec pubcomp_packet(any) :: Exmqtt.Packet.Mqtt.t()
  def pubcomp_packet(packet_id) do
    %Mqtt{
      header: %Header{type: Const.pubcomp()},
      variable: %Puback{
        packet_id: packet_id,
        reason_code: 0
      }
    }
  end

  @spec pubcomp_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def pubcomp_packet(packet_id, reason_code) do
    %Mqtt{
      header: %Header{type: Const.pubcomp()},
      variable: %Puback{
        packet_id: packet_id,
        reason_code: reason_code
      }
    }
  end

  @spec pubcomp_packet(any, any, any) :: Exmqtt.Packet.Mqtt.t()
  def pubcomp_packet(packet_id, reason_code, properties) do
    %Mqtt{
      header: %Header{type: Const.pubcomp()},
      variable: %Puback{
        packet_id: packet_id,
        reason_code: reason_code,
        properties: properties
      }
    }
  end

  @spec subscribe_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def subscribe_packet(packet_id, topic_filters) do
    %Mqtt{
      header: %Header{
        type: Const.subscribe(),
        qos: Const.qos_1()
      },
      variable: %Subscribe{
        packet_id: packet_id,
        topic_filters: topic_filters
      }
    }
  end

  @spec subscribe_packet(any, any, any) :: Exmqtt.Packet.Mqtt.t()
  def subscribe_packet(packet_id, properties, topic_filters) do
    %Mqtt{
      header: %Header{
        type: Const.subscribe(),
        qos: Const.qos_1()
      },
      variable: %Subscribe{
        packet_id: packet_id,
        properties: properties,
        topic_filters: topic_filters
      }
    }
  end

  @spec suback_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def suback_packet(packet_id, reason_code) do
    %Mqtt{
      header: %Header{type: Const.suback()},
      variable: %Suback{
        packet_id: packet_id,
        reason_code: reason_code
      }
    }
  end

  def suback_packet(packet_id, properties, reason_code) do
    %Mqtt{
      header: %Header{type: Const.suback()},
      variable: %Suback{
        packet_id: packet_id,
        properties: properties,
        reason_code: reason_code
      }
    }
  end

  @spec unsubscribe_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def unsubscribe_packet(packet_id, topic_filters) do
    %Mqtt{
      header: %Header{
        type: Const.unsubscribe(),
        qos: Const.qos_1()
      },
      variable: %Unsubscribe{
        packet_id: packet_id,
        topic_filters: topic_filters
      }
    }
  end

  @spec unsubscribe_packet(any, any, any) :: Exmqtt.Packet.Mqtt.t()
  def unsubscribe_packet(packet_id, properties, topic_filters) do
    %Mqtt{
      header: %Header{
        type: Const.unsubscribe(),
        qos: Const.qos_1()
      },
      variable: %Unsubscribe{
        packet_id: packet_id,
        properties: properties,
        topic_filters: topic_filters
      }
    }
  end

  @spec unsuback_packet(any) :: Exmqtt.Packet.Mqtt.t()
  def unsuback_packet(packet_id) do
    %Mqtt{
      header: %Header{type: Const.unsuback()},
      variable: %Unsuback{packet_id: packet_id}
    }
  end

  @spec unsuback_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def unsuback_packet(packet_id, reason_code) do
    %Mqtt{
      header: %Header{type: Const.unsuback()},
      variable: %Unsuback{
        packet_id: packet_id,
        reason_code: reason_code
      }
    }
  end

  @spec unsuback_packet(any, any, any) :: Exmqtt.Packet.Mqtt.t()
  def unsuback_packet(packet_id, properties, reason_code) do
    %Mqtt{
      header: %Header{type: Const.unsuback()},
      variable: %Unsuback{
        packet_id: packet_id,
        properties: properties,
        reason_code: reason_code
      }
    }
  end

  @spec disconnect_packet :: Exmqtt.Packet.Mqtt.t()
  def disconnect_packet() do
    %Mqtt{
      header: %Header{type: Const.disconnect()},
      variable: %Disconnect{reason_code: 0}
    }
  end

  @spec disconnect_packet(any) :: Exmqtt.Packet.Mqtt.t()
  def disconnect_packet(reason_code) do
    %Mqtt{
      header: %Header{type: Const.disconnect()},
      variable: %Disconnect{reason_code: reason_code}
    }
  end

  @spec disconnect_packet(any, any) :: Exmqtt.Packet.Mqtt.t()
  def disconnect_packet(reason_code, properties) do
    %Mqtt{
      header: %Header{type: Const.disconnect()},
      variable: %Disconnect{
        reason_code: reason_code,
        properties: properties
      }
    }
  end

  @spec packet(any) :: Exmqtt.Packet.Mqtt.t()
  def packet(type) do
    %Mqtt{
      header: %Header{type: type}
    }
  end
end
