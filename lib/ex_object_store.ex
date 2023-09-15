defmodule ExObjectStore do
  @moduledoc """
  Documentation for `ExObjectStore`.
  """

  alias ExAws.S3
  alias ExObjectStore.Object

  require Logger

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
  Return a stream of all the objects matching the prefix
  """
  @spec stream_prefix(prefix :: String.t()) :: Enumerable.t()
  def stream_prefix(prefix \\ "") do
    root_bucket()
    |> ExAws.S3.list_objects_v2(prefix: prefix)
    |> ExAws.stream!()
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

  @spec root_bucket() :: String.t()
  def root_bucket do
    Application.get_env(:ex_object_store, :root_bucket)
  end
end
