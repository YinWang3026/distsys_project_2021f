defmodule MerkleTreeTest do
    use ExUnit.Case
    doctest MerkleTree
    import Kernel,
        except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
    # defp create_empty_log() do
    #     Raft.new_configuration([:a, :b, :c], :a, 100, 1000, 20)
    # end

    # defp create_nop_log(last_index) do
    #     config = create_empty_log()
    #     log = for idx <- last_index..1, do: Raft.LogEntry.nop(idx, 1, :a)
    #     %{config | log: log}
    # end

    test "Create MerkleTree" do
        aTree = MerkleTree.new()
        assert Kernel.length(aTree.matrix) == 0
        assert aTree.root_level == 0
        assert aTree.leaf_count == 0
    end
end