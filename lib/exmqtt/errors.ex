defmodule Exmqtt.Errors do
  defmodule BadProperty do
    defexception [:message]

    @impl true
    def exception(value) do
      msg = "bad property, got #{inspect(value)}"
      %BadProperty{message: msg}
    end
  end

  defmodule BadClientID do
    defexception [:message]

    @impl true
    def exception(_value) do
      msg = "bad client id"
      %BadClientID{message: msg}
    end
  end

  defmodule BadPacketID do
    defexception [:message]

    @impl true
    def exception(_value) do
      msg = "bad packet id"
      %BadPacketID{message: msg}
    end
  end

  defmodule BadFrameHeader do
    defexception [:message]

    @impl true
    def exception(_value) do
      msg = "bad frame header"
      %BadFrameHeader{message: msg}
    end
  end

  defmodule FrameTooLarge do
    defexception [:message]

    @impl true
    def exception(_value) do
      msg = "mqtt frame too large"
      %FrameTooLarge{message: msg}
    end
  end

  defmodule UTF8Undifined do
    defexception [:message]

    @impl true
    def exception(_value) do
      msg = "utf8 string undefined"
      %UTF8Undifined{message: msg}
    end
  end

  defmodule BadSubQoS do
    defexception [:message]

    @impl true
    def exception(_value) do
      msg = "bad subqos"
      %BadSubQoS{message: msg}
    end
  end

end
