defmodule DynamoNodeTest do
    use ExUnit.Case
    doctest DynamoNode
  
    require Logger
  
    import Emulation, only: [spawn: 2, send: 2]
  
    import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  
    alias ExHashRing.HashRing, as: Ring
  
    setup do
      Emulation.init()
      Emulation.append_fuzzers([Fuzzers.delay_map(%{"CoordinatorGetRequest" => 5})])
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

  test "Replicas sync after a while" do
    nodes = [:a, :b, :c, :d]
    n = 3
    w = 3
    DynamoUtils.new_cluster(%{foo: 42}, nodes, n, w, w, 9999, 9999, 200, 500, 200)

    pref_list =
      Ring.find_nodes(Ring.new(nodes, 1), :foo, Enum.count(nodes))

    Logger.debug("preference list: #{inspect(pref_list)}")

    [pref_1, pref_2, _pref_3, pref_4] = pref_list

    send(pref_2, :crash)

    # send get request so coordinator knows pref_2 has crashed
    send(pref_1, %ClientGetRequest{nonce: DynamoUtils.generate_nonce(), key: :foo})
    wait(1000)

    # send put request to establish hinted data at pref_4
    put_nonce = DynamoUtils.generate_nonce()

    send(pref_1, %ClientPutRequest{
      nonce: put_nonce,
      key: :foo,
      value: 49,
      context: new_context()
    })

    wait(500)

    # hint should be present at pref_4 by now
    # now crash it before it can hand off
    send(pref_4, :crash)
    wait(200)

    # crashed node now recovers
    send(pref_2, :recover)

    # other nodes should figure this out after a while due to alive_check_interval
    wait(500 + 200)

    # after a while, one of pref_1 and pref_3 should sync with pref_2 (or vice-versa)
    wait(600)

    # pref_2 should now have been synced
    test_nonce = DynamoUtils.generate_nonce()
    send(pref_2, %GetStateRequest{nonce: test_nonce})
    assert_receive %GetStateResponse{nonce: ^test_nonce, state: pref_2_state}, 500

    assert {[49], _context} = Map.get(pref_2_state.store, :foo)
  end
    test "Put request overwrites key in initial data" do
      DynamoUtils.new_cluster(
        %{foo: 37},
        [:a, :b, :c],
        1,
        1,
        1,
        1_000,
        1_000,
        9999,
        500,
        700
      )
  
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
  
    test "Simple get with multiple nodes" do
      data = %{foo: 39, bar: 42, baz: 47}
  
      nodes = [:a, :b, :c, :d, :e, :f]
  
      DynamoUtils.new_cluster(data, nodes, 4, 3, 2, 1_000, 1_000, 9999, 500, 700)
  
      for {key, value} <- data do
        nonce = DynamoUtils.generate_nonce()
  
        send(:c, %ClientGetRequest{nonce: nonce, key: key})
  
        assert_receive %ClientGetResponse{
                         nonce: ^nonce,
                         success: true,
                         values: [^value],
                         context: _context
                       },
                       5_000
      end
    end
  
    test "Simple put and get with multiple nodes" do
      data = %{foo: 39, bar: 42, baz: 47}
  
      nodes = [:a, :b, :c, :d, :e, :f]
  
      DynamoUtils.new_cluster(data, nodes, 4, 3, 2, 1_000, 1_000, 9999, 500, 700)
  
      for {key, _value} <- data do
        nonce = DynamoUtils.generate_nonce()
  
        send(:c, %ClientPutRequest{
          nonce: nonce,
          key: key,
          value: 100,
          context:  %Context{version: VectorClock.new()}
        })
      end
  
      # wait a bit for the system to settle
      wait(1_000)
  
      for key <- Map.keys(data) do
        nonce = DynamoUtils.generate_nonce()
  
        send(:e, %ClientGetRequest{nonce: nonce, key: key})
  
        assert_receive %ClientGetResponse{
                         nonce: ^nonce,
                         success: true,
                         values: [100],
                         context: _context
                       },
                       5_000
      end
    end
  
    test "Client get request times out after a while when sent to coordinator" do
      # make sure the node we send to is a valid coordinator
      # by making everyone a valid coordinator
      spawn(:a, fn ->
        DynamoNode.init(
          :a,
          %{foo: 42},
          [:a, :b, :c],
          3,
          3,
          3,
          500,
          1_000,
          9999,
          500,
          700
        )
      end)
  
      nonce = DynamoUtils.generate_nonce()
      send(:a, %ClientGetRequest{nonce: nonce, key: :foo})
  
      assert_receive %ClientGetResponse{
                       nonce: ^nonce,
                       success: false,
                       values: nil,
                       context: nil
                     },
                     1_000
    end
  
    test "Client put request times out after a while when sent to coordinator" do
      # make sure the node we send to is a valid coordinator
      # by making everyone a valid coordinator
      spawn(:a, fn ->
        DynamoNode.init(
          :a,
          %{foo: 42},
          [:a, :b, :c],
          3,
          3,
          3,
          500,
          1_000,
          9999,
          500,
          700
        )
      end)
  
      nonce = DynamoUtils.generate_nonce()
  
      send(:a, %ClientPutRequest{
        nonce: nonce,
        key: :foo,
        value: 49,
        context: new_context()
      })
  
      assert_receive %ClientPutResponse{
                       nonce: ^nonce,
                       success: false,
                       context: nil
                     },
                     1_000
    end

    test "Follower considers crashed coordinator dead after trying to redirect" do
      data = Map.new(1..100, fn key -> {key, key * 42} end)
  
      DynamoUtils.new_cluster(
        data,
        [:a, :gonna_crash],
        1,
        1,
        1,
        1_000,
        1_000,
        200,
        500,
        700
      )
  
      send(:gonna_crash, :crash)
  
      # generate bunch of traffic so that :gonna_crash becomes
      # a coordinator at least once
      Enum.each(data, fn {key, _val} ->
        send(:a, %ClientGetRequest{
          nonce: DynamoUtils.generate_nonce(),
          key: key
        })
      end)
  
      # wait for the dust to settle
      wait(1_200)
  
      nonce = DynamoUtils.generate_nonce()
      send(:a, %GetStateRequest{nonce: nonce})
      assert_receive %GetStateResponse{nonce: ^nonce, state: state}, 500
      assert state.alive_nodes == %{gonna_crash: false}
    end
end
  