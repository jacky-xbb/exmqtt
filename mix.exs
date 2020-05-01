defmodule Exmqtt.MixProject do
  use Mix.Project

  def project do
    [
      app: :exmqtt,
      version: "0.1.0",
      elixir: "~> 1.10",
      description: description(),
      package: package(),
      config_path: "./config/config.exs",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      name: "Exmqtt",
      source_url: "https://github.com/brianbinbin/exmqtt"
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gen_state_machine, "~> 2.1"},
      {:gun, "~> 1.3.0"},
    ]
  end

  defp description() do
    "Elixir MQTT v5.0 Client"
  end

  defp package() do
    [
      maintainers: ["Brian Bian"],
      # These are the default files included in the package
      files: ~w(config lib .formatter.exs mix.exs README* LICENSE*),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/brianbinbin/exmqtt"}
    ]
  end
end
