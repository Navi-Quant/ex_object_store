ExUnit.start()

ExObjectStore.Repo.start_link()
Ecto.Adapters.SQL.Sandbox.mode(ExObjectStore.Repo, :manual)
