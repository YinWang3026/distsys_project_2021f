defmodule DynamoNodeTest do
    use ExUnit.Case
    doctest DynamoNode
  
    require Logger
  
    import Emulation, only: [spawn: 2, send: 2]
  
    import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  
    alias ExHashRing.Ring
  
    setup do
      Emulation.init()
      Process.register(self(), :test_proc)
      Emulation.mark_unfuzzable()
      :ok
    end
  
    # Wait for timeout milliseconds
    defp wait(timeout) do
      receive do
      after
        timeout -> true
      end
    end
  
    defp new_context() do
      %Context{version: VectorClock.new()}
    end
  
    describe "No crashes" do
      test "during startup of a single node" do
        handle =
          Process.monitor(
            spawn(:node, fn ->
              DynamoNode.init(
                :node,
                %{},
                [:node],
                1,
                1,
                1,
                1_000,
                1_000,
                9999,
                500,
                700
              )
            end)
          )
  
        receive do
          {:DOWN, ^handle, _, proc, reason} ->
            assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
        after
          2_000 ->
            true
        end
      end
  
      test "during startup of multiple nodes" do
        nodes = [:a, :b, :c]
  
        for node <- nodes do
          Process.monitor(
            spawn(node, fn ->
              DynamoNode.init(
                node,
                %{},
                nodes,
                1,
                1,
                1,
                1_000,
                1_000,
                9999,
                500,
                700
              )
            end)
          )
        end
  
        receive do
          {:DOWN, _handle, _, proc, reason} ->
            assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
        after
          2_000 ->
            true
        end
      end
  
      test "on a get request" do
        nodes = [:a, :b, :c]
  
        for node <- nodes do
          Process.monitor(
            spawn(node, fn ->
              DynamoNode.init(
                node,
                %{foo: 42, bar: 62},
                nodes,
                1,
                1,
                1,
                1_000,
                1_000,
                9999,
                500,
                700
              )
            end)
          )
        end
  
        send(:a, %ClientGetRequest{nonce: DynamoUtils.generate_nonce(), key: :foo})
  
        receive do
          {:DOWN, _handle, _, proc, reason} ->
            assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
        after
          2_000 ->
            true
        end
      end
  
      test "on a put request" do
        nodes = [:a, :b, :c]
  
        for node <- nodes do
          Process.monitor(
            spawn(node, fn ->
              DynamoNode.init(
                node,
                %{foo: 42},
                nodes,
                1,
                1,
                1,
                1_000,
                1_000,
                9999,
                500,
                700
              )
            end)
          )
        end
  
        send(:a, %ClientPutRequest{
          nonce: DynamoUtils.generate_nonce(),
          key: :foo,
          value: 49,
          context: %Context{version: VectorClock.new()}
        })
  
        receive do
          {:DOWN, _handle, _, proc, reason} ->
            assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
        after
          2_000 ->
            true
        end
      end
    end
  
    test "First get request returns the initial value" do
      DynamoUtils.new_cluster(
        %{foo: 42},
        [:a, :b, :c],
        3,
        2,
        2,
        1_000,
        1_000,
        9999,
        500,
        700
      )
  
      nonce = DynamoUtils.generate_nonce()
      send(:a, %ClientGetRequest{nonce: nonce, key: :foo})
  
      assert_receive %ClientGetResponse{
                       nonce: ^nonce,
                       success: true,
                       values: [42],
                       context: _context
                     },
                     5_000
    end
  
    test "Simple put request is successful" do
      DynamoUtils.new_cluster(%{}, [:a, :b, :c], 1, 1, 1, 1_000, 1_000, 9999, 500, 700)
      nonce = DynamoUtils.generate_nonce()
  
      send(:a, %ClientPutRequest{
        nonce: nonce,
        key: :foo,
        value: 42,
        context: %Context{version: VectorClock.new()}
      })
  
      assert_receive %ClientPutResponse{nonce: ^nonce, success: true},
                     5_000
    end
  
    test "get after a put returns the put value with empty initial data" do
      DynamoUtils.new_cluster(%{}, [:a, :b, :c], 1, 1, 1, 1_000, 1_000, 9999, 500, 700)
  
      nonce_put = DynamoUtils.generate_nonce()
      nonce_get = DynamoUtils.generate_nonce()
  
      send(:a, %ClientPutRequest{
        nonce: nonce_put,
        key: :foo,
        value: 42,
        context: %Context{version: VectorClock.new()}
      })
  
      send(:a, %ClientGetRequest{nonce: nonce_get, key: :foo})
  
      assert_receive %ClientGetResponse{
                       nonce: ^nonce_get,
                       success: true,
                       values: [42],
                       context: _context
                     },
                     5_000
    end
end
  