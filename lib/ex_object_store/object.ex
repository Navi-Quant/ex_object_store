defmodule ExObjectStore.Object do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  schema "objects" do
    field :name, :string
    field :key, :string

    timestamps()
  end

  def changeset(object, attrs) do
    object
    |> cast(attrs, [:name, :key])
    |> validate_required([:name, :key])
    |> unique_constraint(:key)
  end
end
