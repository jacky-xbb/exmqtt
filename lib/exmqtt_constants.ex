defmodule ExmqttConstants do
  use Constants

  # ------------------------------------------------------
  # MQTT QoS Levels
  # ------------------------------------------------------

  # At most once
  define(qos_0, 0)
  # At least once
  define(qos_1, 1)
  # Exactly once
  define(qos_2, 2)

  # ------------------------------------------------------
  # MQTT Control Packet Types
  # ------------------------------------------------------

  # Reserved
  define(reserved, 0)
  # Client request to connect to Server
  define(connect, 1)
  # Server to Client: Connect acknowledgment
  define(connack, 2)
  # Publish message
  define(publish, 3)
  # Publish acknowledgment
  define(puback, 4)
  # Publish received (assured delivery part 1)
  define(pubrec, 5)
  # Publish release (assured delivery part 2)
  define(pubrel, 6)
  # Publish complete (assured delivery part 3)
  define(pubcomp, 7)
  # Client subscribe request
  define(subscribe, 8)
  # Server Subscribe acknowledgment
  define(suback, 9)
  # Unsubscribe request
  define(unsubscribe, 10)
  # Unsubscribe acknowledgment
  define(unsuback, 11)
  # PING request
  define(pingreq, 12)
  # PING response
  define(pingresp, 13)
  # Client or Server is disconnecting
  define(disconnect, 14)
  # Authentication exchange
  define(auth, 15)

  # ------------------------------------------------------
  # MQTT V5.0 Reason Codes
  # ------------------------------------------------------
  define(rc_success,                                0x00)
  define(rc_normal_disconnection,                   0x00)
  define(rc_granted_qos_0,                          0x00)
  define(rc_granted_qos_1,                          0x01)
  define(rc_granted_qos_2,                          0x02)
  define(rc_disconnect_with_will_message,           0x04)
  define(rc_no_matching_subscribers,                0x10)
  define(rc_no_subscription_existed,                0x11)
  define(rc_continue_authentication,                0x18)
  define(rc_re_authenticate,                        0x19)
  define(rc_unspecified_error,                      0x80)
  define(rc_malformed_packet,                       0x81)
  define(rc_protocol_error,                         0x82)
  define(rc_implementation_specific_error,          0x83)
  define(rc_unsupported_protocol_version,           0x84)
  define(rc_client_identifier_not_valid,            0x85)
  define(rc_bad_user_name_or_password,              0x86)
  define(rc_not_authorized,                         0x87)
  define(rc_server_unavailable,                     0x88)
  define(rc_server_busy,                            0x89)
  define(rc_banned,                                 0x8A)
  define(rc_server_shutting_down,                   0x8B)
  define(rc_bad_authentication_method,              0x8C)
  define(rc_keep_alive_timeout,                     0x8D)
  define(rc_session_taken_over,                     0x8E)
  define(rc_topic_filter_invalid,                   0x8F)
  define(rc_topic_name_invalid,                     0x90)
  define(rc_packet_identifier_in_use,               0x91)
  define(rc_packet_identifier_not_found,            0x92)
  define(rc_receive_maximum_exceeded,               0x93)
  define(rc_topic_alias_invalid,                    0x94)
  define(rc_packet_too_large,                       0x95)
  define(rc_message_rate_too_high,                  0x96)
  define(rc_quota_exceeded,                         0x97)
  define(rc_administrative_action,                  0x98)
  define(rc_payload_format_invalid,                 0x99)
  define(rc_retain_not_supported,                   0x9A)
  define(rc_qos_not_supported,                      0x9B)
  define(rc_use_another_server,                     0x9C)
  define(rc_server_moved,                           0x9D)
  define(rc_shared_subscriptions_not_supported,     0x9E)
  define(rc_connection_rate_exceeded,               0x9F)
  define(rc_maximum_connect_time,                   0xA0)
  define(rc_subscription_identifiers_not_supported, 0xA1)
  define(rc_wildcard_subscriptions_not_supported,   0xA2)

  # ------------------------------------------------------
  # MQTT Protocol Version and Names
  # ------------------------------------------------------
  define(mqtt_proto_v3, 3)
  define(mqtt_proto_v4, 4)
  define(mqtt_proto_v5, 6)

  # ------------------------------------------------------
  # MQTT Frame Mask
  # ------------------------------------------------------
  define(highbit, 0b10000000)
  define(lowbits, 0b01111111)

  # ------------------------------------------------------
  # Maximum MQTT Packet ID and Length
  # ------------------------------------------------------
  define(max_packet_id, 0xffff)
  define(max_packet_size, 0xfffffff)

  # ------------------------------------------------------
  # Default timeout
  # ------------------------------------------------------
  define(default_keepalive, 60)
  define(default_ack_timeout, 30000)
  define(default_connect_timeout, 60000)

  define(no_client_id, <<>>)

  define(no_msg_hdlr, nil)
end
