defmodule Exmqtt.MixProject do
  use Mix.Project

  def project do
    [
      app: :exmqtt,
      version: "0.1.0",
      elixir: "~> 1.10",
      config_path: "./config/config.exs",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
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
      {:gen_state_machine, "~> 2.1", only: [:dev, :prod]},
      {:gun, "~> 1.3", override: true, only: [:dev, :prod]},
      {:meck, github: "eproxus/meck", tag: "0.8.13", only: [:test]},
      {:emqx, github: "emqx/emqx", branch: "master", only: [:test]},
      {:emqx_ct_helpers, github: "emqx/emqx-ct-helpers", tag: "1.2.1", only: [:test]},
    ]
  end
end
