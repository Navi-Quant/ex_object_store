defmodule ExObjectStore do
  @moduledoc """
  Documentation for `ExObjectStore`.
  """

  alias ExObjectStore.Object

  def list_objects do
    repo().all(Object)
  end

  def create_object(attrs \\ %{}) do
    %Object{}
    |> Object.changeset(attrs)
    |> repo().insert()
  end

  defp repo do
    Application.get_env(:ex_object_store, :repo)
  end
end
