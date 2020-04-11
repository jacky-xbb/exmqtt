defmodule ExmqttTest do
  use ExUnit.Case
  doctest Exmqtt

  test "greets the world" do
    assert Exmqtt.hello() == :world
  end
end
