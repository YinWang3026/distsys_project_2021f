# iex(6)> <<5::3>> == <<5::3>>
# true
# iex(7)> <<5::3>> == <<4::3>> 
# false
# iex(8)> <<5::3>> < <<4::3>> 
# false
# iex(9)> <<5::3>> > <<4::3>>
# true
# iex(10)>
# nil
# iex(11)> x =  :crypto.hash(:md5, "hi")
# <<73, 246, 138, 92, 132, 147, 236, 44, 11, 244, 137, 130, 28, 33, 252, 59>>
# iex(12)> y =  :crypto.hash(:md5, "hello")
# <<93, 65, 64, 42, 188, 75, 42, 118, 185, 113, 157, 145, 16, 23, 197, 146>>
# iex(13)> x == y
# false
# iex(14)> x === y
# false
# iex(15)> x < y
# true
# iex(16)> x > y 
# false

## Note this is Binary Search Tree
## May or may not be balanced
## Each Node holds a hash, which is the combined hash of itself and its children
## Each Node holds a value, which is the hash value of some value stored in map

defmodule MerkleTree do
    @moduledoc """
    MerkleTree implementation.
    """
    import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
    alias __MODULE__
    @type bytes :: <<_::_*8>>
    @enforce_keys [:matrix, :root_level, :leaf_count]
    defstruct(
        matrix: nil, # The matrix holding the tree
        root_level: nil, # Which level is the root node at
        leaf_count: nil # How many leafs
    )

    ## Level 0 = Data nodes
    ## root_level has root node

    @doc """
    Return an empty Merkle Tree, this is mostly
    used for convenience.
    """
    @spec new() :: %MerkleTree{
                        matrix: list(),
                        root_level: non_neg_integer(),
                        leaf_count: non_neg_integer()
                    }
    def new() do
        %MerkleTree{matrix: [], root_level: 0, leaf_count: 0 }
    end

    # @doc """
    # Return a Merkle Tree with the new value inserted.
    # """
    # @spec insert(bytes) :: %MerkleTree{}
    # def insert(bytes) do

    # end

    # @doc """
    # Return the root hash.
    # """
    # @spec get_root_hash(%MerkleTree) :: bytes
    # def insert(tree) do

    # end
end

