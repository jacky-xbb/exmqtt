import Config

config :logger, :console,
  level: :debug,
  format: {Exmqtt.LogFormatter, :format},
  metadata: [:client_id]
