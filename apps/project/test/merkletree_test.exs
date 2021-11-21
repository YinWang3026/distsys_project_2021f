defmodule MerkleTreeTest do

    use ExUnit.Case

    doctest MerkleTree

    import Kernel,
        except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

    test "Create MerkleTree" do
        aTree = MerkleTree.new()
        assert length(aTree.matrix) == 1
        assert aTree.root_level == 0
        assert aTree.leaf_count == 0
    end
    
    ## <<val::size>> Bitstring constructor, default size = 8 bits = 1 byte
    test "Insert MerkleTree and get Root hash" do
        aTree = MerkleTree.new()
        root_hash = MerkleTree.get_root_hash(aTree)
        assert root_hash == :no_root

        # 128 bits for md5
        h1 = :crypto.hash(:md5, <<"HI">>) # <<191, 140, 20, 65, 64, 177, 91, 239, 184, 206, 102, 38, 50, 167, 183, 110>>
        h2 = :crypto.hash(:md5, <<"I AM YIN">>) # <<87, 232, 159, 168, 114, 169, 139, 69, 126, 98, 118, 150, 100, 207, 73, 34>>
        h3 = :crypto.hash(:md5, <<"THIS IS DIST SYS">>) # <<202, 131, 189, 86, 8, 5, 202, 50, 31, 118, 229, 137, 204, 6, 23, 78>>
        h4 = :crypto.hash(:md5, <<"PROJECT DYNAMO">>) # <<33, 227, 233, 6, 27, 75, 153, 23, 0, 222, 243, 76, 127, 212, 173, 75>>
        h5 = :crypto.hash(:md5, <<12,23,45,56>>) # <<212, 84, 233, 244, 41, 222, 8, 168, 214, 184, 144, 186, 6, 226, 191, 226>>

        # Insert item 1
        aTree = MerkleTree.insert(aTree, h1)
        assert length(aTree.matrix) == 2
        assert aTree.root_level == 1
        assert aTree.leaf_count == 1
        root_hash = MerkleTree.get_root_hash(aTree)
        assert root_hash == h1

        # Insert item 2
        aTree = MerkleTree.insert(aTree, h2)
        assert length(aTree.matrix) == 2
        assert aTree.root_level == 1
        assert aTree.leaf_count == 2
        root_hash = MerkleTree.get_root_hash(aTree)
        h1_h2 = :crypto.hash(:md5, h1 <> h2)
        assert root_hash == h1_h2

        # Insert item 3
        aTree = MerkleTree.insert(aTree, h3)
        assert length(aTree.matrix) == 3
        assert aTree.root_level == 2
        assert aTree.leaf_count == 3
        root_hash = MerkleTree.get_root_hash(aTree)
        h1_h2_h3 = :crypto.hash(:md5, h1_h2 <> h3)
        assert root_hash == h1_h2_h3

        # Insert item 4
        aTree = MerkleTree.insert(aTree, h4)
        assert length(aTree.matrix) == 3
        assert aTree.root_level == 2
        assert aTree.leaf_count == 4
        root_hash = MerkleTree.get_root_hash(aTree)
        h3_h4 = :crypto.hash(:md5, h3 <> h4)
        h1_h2_h3_h4 = :crypto.hash(:md5, h1_h2 <> h3_h4)
        assert root_hash == h1_h2_h3_h4

        # Insert item 5
        aTree = MerkleTree.insert(aTree, h5)
        assert length(aTree.matrix) == 4
        assert aTree.root_level == 3
        assert aTree.leaf_count == 5
        root_hash = MerkleTree.get_root_hash(aTree)
        assert root_hash == :crypto.hash(:md5, h1_h2_h3_h4 <> h5)
        
        # Insert none binary item
        assert_raise FunctionClauseError, fn ->
            MerkleTree.insert(aTree, 12234556) # Inserting something not a byte
        end

    end
end