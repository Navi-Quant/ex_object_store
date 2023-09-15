defmodule ExObjectStore.GarbageCollector do
  @moduledoc """
  Genserver that deletes all the objects that are not referenced in the object sync
  on an interval
  """

  use GenServer

  alias ExObjectStore

  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    interval = Keyword.get(opts, :interval, 60_000)
    object_sync = Keyword.get(opts, :object_sync)
    prefixes = Keyword.get(opts, :prefixes, [""])

    state = %{interval: interval, object_sync: object_sync, prefixes: prefixes}
    schedule_work(state)

    {:ok, state}
  end

  def handle_info(:work, state) do
    Logger.info("Running garbage collection")

    for prefix <- state.prefixes do
      case ExObjectStore.delete_garbage(prefix, object_sync: state.object_sync) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.error("Failed to delete objects for prefix #{prefix}: #{inspect(reason)}")
      end
    end

    schedule_work(state)
    {:noreply, state}
  end

  defp schedule_work(state) do
    Process.send_after(self(), :work, state.interval)
  end
end
