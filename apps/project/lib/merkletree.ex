## Tree is built from bottom up and left to right
## Leaves are the hashes of some values
## Each internal node is the md5(left_child <> right_child)

defmodule MerkleTree do
    @moduledoc """
    MerkleTree implementation.
    """
    import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
    
    alias __MODULE__ 
    
    @type t() :: %__MODULE__{
        matrix: list(list(any())),
        root_level: non_neg_integer(),
        leaf_count: non_neg_integer()
    }

    @enforce_keys [:matrix, :root_level, :leaf_count]
    defstruct(
        matrix: nil, # The matrix holding the tree
        root_level: nil, # Which level is the root node at
        leaf_count: nil # How many leafs
    )

    ## Level 0 = Data nodes
    ## root_level has the root node

    @doc """
    Return an empty Merkle Tree, this is mostly
    used for convenience.
    """
    @spec new() :: %MerkleTree{
        matrix: list(list(any())),
        root_level: non_neg_integer(),
        leaf_count: non_neg_integer()
    }
    def new() do
        %MerkleTree{ matrix: [[]], root_level: 0, leaf_count: 0 }
    end

    ## Given a matrix, row, and a value
    ## Returns a matrix with the value appended to the row
    defp append_to_row(matrix, row, value) do
        List.replace_at(matrix, row, Enum.at(matrix, row) ++ [value])
    end
    
    ## Given a matrix, row, and col
    ## Returns the item at row, col in the matrix
    defp get_item_at(matrix, row, col) do
        Enum.at(Enum.at(matrix, row), col)
    end

    ## Given a matrix, row, col, and value
    ## Returns a matrix with value at row, col
    defp replace_item_at(matrix, row, col, value) do
        row_list = List.replace_at(Enum.at(matrix, row), col, value)
        List.replace_at(matrix, row, row_list)
    end

    ## Given 2 hashes, h1 and h2
    ## Return if h2 == nil, return h1, else return md5(h1 <> h2)
    defp concatenate_hash(h1, h2) do
        if h2 == nil do
            h1
        else
            :crypto.hash(:md5, h1 <> h2)
        end
    end

    defp insert_rec(matrix, row, col) do
        # IO.puts("matrix inspection #{inspect(matrix)}")
        if length(matrix) <= row do
            ## Need to add a row
            ## Generally, this is a top/root, so we are done
            h1 = get_item_at(matrix, row-1, col*2)
            h2 = get_item_at(matrix, row-1, col*2+1)
            new_hash = concatenate_hash(h1, h2)
            new_matrix = matrix ++ [[new_hash]]
            {new_matrix, row}
        else
            ## Row exists
            if length(Enum.at(matrix, row)) <= col do
                ## Parent node does not exist, add it, and copy its child hash
                child_hash = get_item_at(matrix, row-1, col*2)
                new_matrix = append_to_row(matrix, row, child_hash)
                insert_rec(new_matrix, row+1, Integer.floor_div(col, 2))
            else
                ## Parent node does exist
                ## Recalculate its hash
                h1 = get_item_at(matrix, row-1, col*2)
                h2 = get_item_at(matrix, row-1, col*2+1) # h2 might be nil, so only take on h1
                new_hash = concatenate_hash(h1, h2)
                new_matrix = replace_item_at(matrix, row, col, new_hash)
                if length(Enum.at(new_matrix, row)) == 1 do
                    {new_matrix, row}
                else
                    insert_rec(new_matrix, row+1, Integer.floor_div(col, 2))
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
        # IO.puts("Inserting ...")
        # IO.inspect(bytes, binaries: :as_binaries)

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
        # IO.puts("new_matrix #{inspect(new_matrix)} new_root_level #{inspect(new_root_level)}")

        ## Update
        tree = %{ tree | matrix: new_matrix }
        tree = %{ tree | leaf_count: new_leaf_count }
        %{ tree | root_level: new_root_level }
    end

    @doc """
    Return the root hash.
    """
    @spec get_root_hash(%MerkleTree{}) :: :no_root | binary()
    def get_root_hash(tree) do
        if tree.root_level == 0 do
            ## Empty tree, no root
            :no_root
        else
            ## Return the first item on root_level
            root_list = Enum.at(tree.matrix, tree.root_level)
            if length(root_list) != 1 do 
                ## Error check in case
                :no_root
            else 
                ## Root exists, get the first item in the list
                Enum.at(root_list, 0)
            end
        end
    end

    @doc """
    Called by the reciever, tree_receiver
    sender's tree, tree_sender
    If my tree is not as update to date, request data
    If my tree is more update, do nothing
    """
    @spec compare_tree(%MerkleTree{}, %MerkleTree{}) :: :same | :do_nothing | non_neg_integer()
    def compare_tree(tree_sender, tree_receiver) do
        cond do
            tree_sender.root_level == 0 and tree_receiver.root_level != 0 ->
                0 # I have more entries, send everything to the sender
            tree_sender.root_level != 0 and tree_receiver.root_level == 0 ->
                :do_nothing
            tree_sender.root_level == 0 and tree_receiver.root_level == 0 ->
                :same
            tree_sender.root_level > tree_receiver.root_level ->
                :do_nothing # Sender has more items than me
            tree_sender.root_level < tree_receiver.root_level ->
                0 # I have more entries, send everything to the sender
            get_root_hash(tree_sender) == get_root_hash(tree_receiver) ->
                # Same root hash, we done
                :same
            true ->
                # Same height, time to compare row by row
                compare_tree_row(tree_sender, tree_receiver, tree_receiver.root_level-1, 0)
        end
    end

    @spec compare_tree_row(%MerkleTree{}, %MerkleTree{}, non_neg_integer(), non_neg_integer())
        :: non_neg_integer()
    defp compare_tree_row(tree_sender, tree_receiver, row, idx) do
        row_sender = Enum.at(tree_sender.matrix, row)
        row_receiver = Enum.at(tree_receiver.matrix, row)
        if row == 0 do
            # Last row, no more to check
            if Enum.at(row_receiver, idx) == Enum.at(row_sender, idx) do
                idx + 1
            else
                idx
            end
        else
            mismatch_idx =
                Enum.zip(row_sender, row_receiver) 
                |> Enum.map(fn {s, r} -> s == r end) 
                |> Enum.with_index()
                |> Enum.reduce_while(nil, fn {r, i}, acc -> if r == true, do: {:cont, acc}, else: {:halt, i} end)
            
            # Calculate the starting mismatch index
            current_idx = mismatch_idx * round(:math.pow(2, row))
            # Take the better idx
            compare_tree_row(tree_sender, tree_receiver, row-1, max(idx, current_idx))
        end
    end
end

