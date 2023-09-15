defmodule ExObjectStore.Repo do
  use Ecto.Repo,
    otp_app: :ex_object_store,
    adapter: Ecto.Adapters.Postgres
end
