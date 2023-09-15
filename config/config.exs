import Config

if config_env() == :test do
  config :ex_object_store,
    root_bucket: "test",
    repo: ExObjectStore.Repo,
    ecto_repos: [ExObjectStore.Repo]

  config :ex_object_store,
         ExObjectStore.Repo,
         username: "postgres",
         password: "password",
         database: "ex_object_store_test",
         hostname: "localhost",
         pool: Ecto.Adapters.SQL.Sandbox

  config :ex_aws,
    access_key_id: "minioadmin",
    secret_access_key: "minioadmin"

  config :ex_aws, :s3,
    scheme: "http://",
    host: "localhost",
    port: 9000
end
