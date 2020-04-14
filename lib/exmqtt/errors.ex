defmodule Exmqtt.Errors do
  defmodule BadProperty do
    defexception [:message]

    @impl true
    def exception(value) do
      msg = "bad property, got #{inspect(value)}"
      %BadProperty{message: msg}
    end
  end
end
