defmodule DynamoUtils do
    @moduledoc """
    Generate pseudo-random nonces for messages.
    """
    
    @max_nonce 999_999_999
  
    @doc """
    Generate a new nonce.
    """
    @spec generate_nonce() :: pos_integer()
    def generate_nonce do
      if :rand.export_seed() == :undefined do
        :rand.seed(:exrop, :erlang.now())
      end
  
      :rand.uniform(@max_nonce)
    end
  end
  