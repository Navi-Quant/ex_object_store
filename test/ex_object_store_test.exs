defmodule ExObjectStoreTest do
  use ExUnit.Case

  defmodule TestSync do
    @moduledoc false
    @behaviour ExObjectStore.ObjectSync

    def live_keys(_prefix) do
      ["test/folder/live_object.txt"]
    end
  end

  describe "s3 handling" do
    setup do
      ExObjectStore.delete_prefix("")
    end

    test "upload_object/4 returns ok with the key when successful" do
      assert {:ok, "folder/test.txt"} = ExObjectStore.upload_object("folder", "test.txt", "test")
      assert {:ok, "test"} = ExObjectStore.download_object("folder/test.txt")
    end

    test "download_objects/1 returns ok with binary contents of zip" do
      {:ok, key1} = ExObjectStore.upload_object("folder", "test.txt", "test")
      {:ok, key2} = ExObjectStore.upload_object("folder", "test2.txt", "test2")

      assert {:ok, zip_binary} = ExObjectStore.download_objects([key1, key2])
      assert {:ok, files} = :zip.extract(zip_binary, [:memory])

      file_map = for {name, binary} <- files, into: %{}, do: {IO.chardata_to_string(name), binary}
      assert file_map["folder/test.txt"] == "test"
      assert file_map["folder/test2.txt"] == "test2"

      # with key tranform
      assert {:ok, zip_binary} =
               ExObjectStore.download_objects([key1, key2], key_transform: &ExObjectStore.strip_key/1)

      assert {:ok, files} = :zip.extract(zip_binary, [:memory])

      file_map = for {name, binary} <- files, into: %{}, do: {IO.chardata_to_string(name), binary}
      assert file_map["test.txt"] == "test"
      assert file_map["test2.txt"] == "test2"
    end

    @tag :tmp_dir
    test "upload_object_from_file/4 returns ok with the key when successful", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "test.txt")
      File.write!(path, "test")

      assert {:ok, "folder/test.txt"} = ExObjectStore.upload_object_from_file("folder", "test.txt", path)
      assert {:ok, "test"} = ExObjectStore.download_object("folder/test.txt")
    end

    test "presigned_url/2 returns the presigned url for object with key" do
      {:ok, key} = ExObjectStore.upload_object("folder", "object.txt", "test")
      {:ok, url} = ExObjectStore.presigned_url(key)
      assert url =~ "http://localhost:9000/test/folder/object.txt"
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

    test "stream_garbage/1 lists all the objects that are unreferenced in local store that are still in s3 with the matching prefix" do
      {:ok, _object} = ExObjectStore.upload_object("test/folder", "live_object.txt", "contents")

      {:ok, garbage_object} = ExObjectStore.upload_object("test/folder", "garbage_object.txt", "contents")
      {:ok, other_folder} = ExObjectStore.upload_object("test/other", "other.txt", "contents")

      assert "test" |> ExObjectStore.stream_prefix() |> Enum.to_list() |> length() == 3

      assert [%{key: ^garbage_object}, %{key: ^other_folder}] =
               "test"
               |> ExObjectStore.stream_garbage(object_sync: TestSync)
               |> Enum.to_list()

      assert [%{key: ^garbage_object}] =
               "test/folder"
               |> ExObjectStore.stream_garbage(object_sync: TestSync)
               |> Enum.to_list()
    end

    test "delete_garbage/1 deletes all the unreferenced objects from s3" do
      {:ok, live_object} = ExObjectStore.upload_object("test/folder", "live_object.txt", "contents")
      {:ok, _garbage_object} = ExObjectStore.upload_object("test/folder", "garbage_object.txt", "contents")

      assert "test" |> ExObjectStore.stream_prefix() |> Enum.to_list() |> length() == 2
      assert :ok == ExObjectStore.delete_garbage("test", object_sync: TestSync)
      assert [live_object] == "test" |> ExObjectStore.stream_prefix() |> Enum.map(& &1.key)
    end

    test "ensure_root_bucket" do
      assert :ok == ExObjectStore.ensure_root_bucket()
      assert ExObjectStore.root_bucket_exists?()
    end
  end

  describe "garbage collector" do
    setup do
      ExObjectStore.delete_prefix("")
    end

    test "deletes objects on interval" do
      {:ok, _live_object} = ExObjectStore.upload_object("test/folder", "live_object.txt", "contents")
      {:ok, _garbage_object} = ExObjectStore.upload_object("test/folder", "garbage_object.txt", "contents")

      assert ExObjectStore.stream_prefix() |> Enum.to_list() |> length() == 2

      # start garbage collector
      start_supervised!({ExObjectStore.GarbageCollector, object_sync: TestSync, interval: 1})
      :timer.sleep(5)

      assert ExObjectStore.stream_prefix() |> Enum.to_list() |> length() == 1
    end
  end
end
