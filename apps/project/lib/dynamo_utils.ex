defmodule DynamoUtils do
    @moduledoc """
    Generate pseudo-random nonces for messages.
    """
    @type nonce_t() :: pos_integer()

    @max_nonce 999_999_999
  
    @doc """
    Generate a new nonce.
    """
    @spec generate_nonce() :: nonce_t()
    def generate_nonce do
      if :rand.export_seed() == :undefined do
        :rand.seed(:exrop, :erlang.now())
      end
  
      :rand.uniform(@max_nonce)
    end
  end
  