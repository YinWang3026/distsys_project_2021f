defmodule DynamoUtils do
    @moduledoc """
    Generate pseudo-random nonces for messages.
    """
    import Emulation, only: [spawn: 2]

    import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  
    require Fuzzers
    require Logger

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

    @doc """
    Start a new Dynamo cluster
    """
    @spec new_cluster(
      map(),
      [any()],
      pos_integer(),
      pos_integer(),
      pos_integer(),
      pos_integer(),
      pos_integer(),
      pos_integer(),
      pos_integer(),
      pos_integer()
    ) ::
      :ok
  def new_cluster(
      data,
      nodes,
      n,
      r,
      w,
      client_timeout,
      redirect_timeout,
      request_timeout,
      health_check_timeout,
      merkle_sync_timeout
    ) do
    for node <- nodes do
      spawn(node, fn ->
        DynamoNode.init(
          node,
          data,
          nodes,
          n,
          r,
          w,
          client_timeout,
          redirect_timeout,
          request_timeout,
          health_check_timeout,
          merkle_sync_timeout
        )
      end)
    end
  end
end
  