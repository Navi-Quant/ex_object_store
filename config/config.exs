import Config

if config_env() == :test do
  config :ex_object_store,
    root_bucket: "test"

  config :ex_aws,
    access_key_id: "minioadmin",
    secret_access_key: "minioadmin"

  config :ex_aws, :s3,
    scheme: "http://",
    host: "localhost",
    port: 9000
end
