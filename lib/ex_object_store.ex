defmodule ExObjectStore do
  @moduledoc """
  Documentation for `ExObjectStore`.
  """

  alias ExAws.S3

  require Logger

  @type object_sync_opt :: {:object_sync, module()}

  @doc """
  Upload a file with name to folder with contents. the folder is just some string
  where "directories" are seperated by slashes. the file name is expected to include the file extension too.
  """
  @spec upload_object(folder :: String.t(), name :: String.t(), contents :: String.t(), opts :: S3.put_object_opts()) ::
          {:ok, String.t()} | {:error, term()}
  def upload_object(folder, name, contents, opts \\ []) do
    key = object_name(folder, name)
    upload = S3.put_object(root_bucket(), key, contents, opts)

    with :ok <- ensure_root_bucket(),
         {:ok, _} <- ExAws.request(upload) do
      {:ok, key}
    end
  end

  defp object_name(folder, file_name), do: folder <> "/" <> file_name

  @doc """
  Upload the file at path to the folder with name
  """
  @spec upload_object_from_file(
          folder :: String.t(),
          name :: String.t(),
          path :: String.t(),
          opts :: S3.put_object_opts()
        ) ::
          {:ok, String.t()} | {:error, term()}
  def upload_object_from_file(folder, name, path, opts \\ []) do
    with {:ok, file_contents} <- File.read(path) do
      upload_object(folder, name, file_contents, opts)
    end
  end

  @doc """
  Return a presigned url for the key
  """
  @spec presigned_url(key :: String.t(), opts :: S3.presigned_url_opts()) :: {:ok, binary()} | {:error, binary()}
  def presigned_url(key, opts \\ []) do
    config = ExAws.Config.new(:s3)
    ExAws.S3.presigned_url(config, :get, root_bucket(), key, opts)
  end

  @doc """
  Download the contents of the object at key
  """
  @spec download_object(key :: String.t()) :: {:ok, binary()} | {:error, term()}
  def download_object(key) do
    download = S3.get_object(root_bucket(), key)

    with {:ok, %{body: body}} <- ExAws.request(download) do
      {:ok, body}
    end
  end

  @doc """
  Download a list of objects by key as a zip file, returning the zip
  binary contents of the zip file.

  ## Options
    * `:key_transform` - a function that takes a key and returns a new key,
      this is useful for stripping out the folder prefix from the key.
      Defaults to `&Function.identity/1`.

    * `:max_concurrency` - the maximum number of concurrent downloads to run.
    * `:timeout` - the timeout for each download (in milliseconds).
  """
  @type download_objects_option ::
          {:key_transform, (String.t() -> String.t())} | {:max_concurrency, pos_integer()} | {:timeout, pos_integer()}

  @spec download_objects(keys :: [String.t()], opts :: [download_objects_option]) :: {:ok, binary()} | {:error, term()}
  def download_objects(keys, opts \\ []) do
    {key_transform, opts} = Keyword.pop(opts, :key_transform, &Function.identity/1)

    with {:ok, objects} <- download_object_binaries(keys, opts) do
      create_object_zip(objects, key_transform)
    end
  end

  defp download_object_binaries(keys, opts) do
    async_opts = opts ++ [on_timeout: :kill_task, zip_input_on_exit: true]

    keys
    |> Task.async_stream(&{&1, download_object(&1)}, async_opts)
    |> Enum.reduce_while([], fn
      {:ok, {key, {:ok, body}}}, acc ->
        {:cont, [{key, body} | acc]}

      {:ok, {key, {:error, error}}}, _acc ->
        {:halt, {:error, {key, error}}}

      {:exit, {key, :timeout}}, _acc ->
        {:halt, {:error, {key, :timeout}}}
    end)
    |> case do
      {:error, error} ->
        {:error, error}

      acc ->
        {:ok, acc}
    end
  end

  defp create_object_zip(objects, key_transform) do
    objects =
      for {key, body} <- objects do
        key = key_transform.(key)
        {String.to_charlist(key), body}
      end

    with {:ok, {~c"archive.zip", zip_binary}} <- :zip.create(~c"archive.zip", objects, [:memory]) do
      {:ok, zip_binary}
    end
  end

  @doc """
  Helper to strip an object key down to its filename
  """
  @spec strip_key(key :: String.t()) :: String.t()
  def strip_key(key) do
    key
    |> String.split("/")
    |> List.last()
  end

  @doc """
  Return a stream of all the objects matching the prefix
  """
  @spec stream_prefix(prefix :: String.t()) :: Enumerable.t()
  def stream_prefix(prefix \\ "") do
    root_bucket()
    |> ExAws.S3.list_objects_v2(prefix: prefix)
    |> ExAws.stream!()
  end

  @doc """
  Return a stream of all the objects that don't have a live key in the object sync

  ## Options
    * `:object_sync` - the object sync module to use, must implement `ExObjectStore.ObjectSync`
  """
  @spec stream_garbage(prefix :: String.t(), opts :: [object_sync_opt()]) :: Enumerable.t()
  def stream_garbage(prefix \\ "", opts \\ []) do
    live_keys =
      opts
      |> Keyword.get(:object_sync, object_sync())
      |> apply(:live_keys, [prefix])
      |> MapSet.new()

    prefix
    |> stream_prefix()
    |> Stream.reject(fn object -> MapSet.member?(live_keys, object.key) end)
  end

  @doc """
  Deletes all objects matching the prefix
  """
  @spec delete_prefix(prefix :: String.t()) :: :ok | {:error, term()}
  def delete_prefix(prefix) do
    prefix
    |> stream_prefix()
    |> Stream.map(& &1.key)
    |> Enum.to_list()
    |> delete_keys()
  end

  @doc """
  Deletes all objects matching the keys
  """
  @spec delete_keys(keys :: [String.t()]) :: :ok | {:error, term()}
  def delete_keys([]), do: :ok

  def delete_keys(keys) do
    delete = S3.delete_all_objects(root_bucket(), keys)

    with {:ok, _} <- ExAws.request(delete) do
      :ok
    end
  end

  @doc """
  Delete all garage objects with matching prefix

  ## Options
    * `:object_sync` - the object sync module to use, must implement `ExObjectStore.ObjectSync`
  """
  @spec delete_garbage(prefix :: String.t(), opts :: [object_sync_opt()]) :: :ok | {:error, term()}
  def delete_garbage(prefix \\ "", opts \\ []) do
    prefix
    |> stream_garbage(opts)
    |> Stream.map(& &1.key)
    |> Enum.to_list()
    |> delete_keys()
  end

  @doc """
  Creates the root bucket if it doesn't exist.
  """
  @spec ensure_root_bucket(any) :: :ok | {:error, term()}
  def ensure_root_bucket(region \\ "") do
    if root_bucket_exists?() do
      :ok
    else
      bucket_create = S3.put_bucket(root_bucket(), region)

      case ExAws.request(bucket_create) do
        {:ok, _} ->
          :ok

        {:error, error} ->
          Logger.error("Failed to create root bucket: #{inspect(error)}")
          {:error, :failed_to_create_root_bucket}
      end
    end
  end

  @doc """
  Does the root bucket exist?
  """
  @spec root_bucket_exists?() :: boolean()
  def root_bucket_exists? do
    bucket_head = S3.head_bucket(root_bucket())

    case ExAws.request(bucket_head) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  defp root_bucket do
    Application.get_env(:ex_object_store, :root_bucket)
  end

  defp object_sync do
    Application.get_env(:ex_object_store, :object_sync)
  end
end
