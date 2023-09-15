defmodule ExObjectStoreTest do
  use ExObjectStore.RepoCase

  describe "objects" do
    alias ExObjectStore.Object

    test "list_objects/0" do
      assert [] == ExObjectStore.list_objects()
    end

    test "create_object/1 with valid data creates a new object" do
      attrs = %{
        name: "My Object",
        key: "my-object"
      }

      assert {:ok, %Object{} = object} = ExObjectStore.create_object(attrs)
      assert object.name == "My Object"
      assert object.key == "my-object"
    end
  end
end
