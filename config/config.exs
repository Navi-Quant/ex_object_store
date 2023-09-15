import Config

if config_env() == :test do
  config :ex_object_store,
    repo: ExObjectStore.Repo,
    ecto_repos: [ExObjectStore.Repo]

  config :ex_object_store,
         ExObjectStore.Repo,
         username: "postgres",
         password: "password",
         database: "ex_object_store_test",
         hostname: "localhost",
         pool: Ecto.Adapters.SQL.Sandbox
end
