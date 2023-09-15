defmodule ExObjectStore.RepoCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  using do
    quote do
      import Ecto
      import Ecto.Query
      import ExObjectStore.RepoCase

      alias ExObjectStore.Repo
    end
  end

  setup tags do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(ExObjectStore.Repo)

    unless tags[:async] do
      Ecto.Adapters.SQL.Sandbox.mode(ExObjectStore.Repo, {:shared, self()})
    end

    :ok
  end
end
