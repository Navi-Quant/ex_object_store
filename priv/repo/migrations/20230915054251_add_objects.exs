defmodule ExObjectStore.Repo.Migrations.AddObjects do
  use Ecto.Migration

  def change do
    create table(:objects) do
      add :name, :string, null: false
      add :key, :string, null: false

      timestamps()
    end

    create unique_index(:objects, [:key])
  end
end
