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

  describe "s3 handling" do
    setup do
      ExObjectStore.delete_prefix("")
    end

    test "upload_object/4 returns ok with the key when successful" do
      assert {:ok, "test/test.txt"} = ExObjectStore.upload_object("test", "test.txt", "test")
    end

    test "stream_prefix/1 returns stream of objects matching prefix" do
      objects = ExObjectStore.stream_prefix("test")
      assert Enum.empty?(Enum.to_list(objects))

      {:ok, _} = ExObjectStore.upload_object("test", "file.txt", "contents")
      objects = ExObjectStore.stream_prefix("test")
      assert [%{key: "test/file.txt"}] = Enum.to_list(objects)

      objects = ExObjectStore.stream_prefix("test/")
      assert [%{key: "test/file.txt"}] = Enum.to_list(objects)

      objects = ExObjectStore.stream_prefix("empty/")
      assert Enum.empty?(objects)
    end

    test "ensure_root_bucket" do
      assert :ok == ExObjectStore.ensure_root_bucket()
      assert ExObjectStore.root_bucket_exists?()
    end
  end
end
