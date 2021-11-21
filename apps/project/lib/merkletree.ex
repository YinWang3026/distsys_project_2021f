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
        %MerkleTree{ matrix: [[]], root_level: 0, leaf_count: 0 }
    end

    defp append_to_row(matrix, row, value) do
        List.replace_at(matrix, row, Enum.at(matrix, row) ++ [value])
    end

    defp append_to_matrix(matrix, value) do
        matrix ++ [value]
    end

    defp get_item_at(matrix, row, col) do
        Enum.at(Enum.at(matrix, row), col)
    end

    defp replace_item_at(matrix, row, col, value) do
        row_list = List.replace_at(Enum.at(matrix, row), col, value)
        List.replace_at(matrix, row, row_list)
    end

    defp insert_rec(matrix, row, col) do
        if length(matrix) <= row do
            # Need to add a row
            h1 = get_item_at(matrix, row-1, col*2)
            h2 = get_item_at(matrix, row-1, col*2+1)
            if h2 == nil do
                new_matrix = matrix ++ [[h1]]
                {new_matrix, row}
            else
                new_hash = :crypto.hash(:md5, h1 <> h2)
                new_matrix = matrix ++ [[new_hash]]
                {new_matrix, row}
            end
        else
            # Row exists
            if length(Enum.at(matrix, row)) <= col do
                # Parent node does not exist
                child_hash = get_item_at(matrix, row-1, col*2)
                matrix = append_to_row(matrix, row, child_hash)
                insert_rec(matrix, row+1, Integer.floor_div(col, 2))
            else
                # Parent node does exist
                h1 = get_item_at(matrix, row-1, col*2)
                h2 = get_item_at(matrix, row-1, col*2+1)
                new_hash = :crypto.hash(:md5, h1 <> h2)
                matrix = replace_item_at(matrix, row, col, new_hash)
                if length(Enum.at(matrix, row)) == 1 do
                    {matrix, row}
                else
                    insert_rec(matrix, row+1, Integer.floor_div(col, 2))
                end
            end
        end
    end

    @doc """
    Return a Merkle Tree with the new value inserted.
    Bytes must be type binary (hashed), aka size % 8 == 0
    """
    @spec insert(%MerkleTree{}, binary()) :: %MerkleTree{
        matrix: list(list()),
        root_level: non_neg_integer(),
        leaf_count: non_neg_integer()
    }
    def insert(tree, bytes) when is_binary(bytes) do
        IO.puts("Insert ...")
        IO.inspect(bytes, binaries: :as_binaries)

        ## Add the new value to 0th row
        new_matrix = append_to_row(tree.matrix, 0, bytes)
        # IO.puts("new_matrix #{inspect(new_matrix)}")
        ## Increase the count
        new_leaf_count = tree.leaf_count + 1

        ## Parent should be at ...
        parent_row = 1
        parent_col = Integer.floor_div(new_leaf_count-1, 2)
        
        ## Recursively fix the tree
        {new_matrix, new_root_level} = insert_rec(new_matrix, parent_row, parent_col)
        
        ## Update
        tree = %{ tree | matrix: new_matrix }
        tree = %{ tree | leaf_count: new_leaf_count }
        tree = %{ tree | root_level: new_root_level }
        # IO.puts("done_matrix #{inspect(new_matrix)}")
        tree
    end

    @doc """
    Return the root hash.
    """
    @spec get_root_hash(%MerkleTree{}) :: :no_root | binary()
    def get_root_hash(tree) do
        if tree.root_level == 0 do
            :no_root
        else
            # Return the first item on root_level
            root_list = Enum.at(tree.matrix, tree.root_level)
            if length(root_list) == 0 do # Error check in case
                :no_root
            else # Root exists
                hd(root_list)
            end
        end
    end

end

