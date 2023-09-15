defmodule ExObjectStore.ObjectSync do
  @moduledoc """
  Defines the behaviour for a module that implements the required functions
  to keep a remote s3 store in sync with some other source (local filesystem or db)
  """

  @doc """
  Return a list of all the "live" keys of objects that should be in the store
  """
  @callback live_keys(prefix :: String.t()) :: [String.t()]
end
