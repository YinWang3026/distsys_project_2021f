defmodule DynamoNode do
  @moduledoc """
  A Dynamo node
  """

  # override Kernel's functions with Emulation's
  import Emulation, only: [send: 2, timer: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  alias ExHashRing.HashRing

  @type t() :: %__MODULE__{
    # Id for dynamo node
    id: any(),

    # Dynamo Parameters N, R and W
    n: pos_integer(),
    r: pos_integer(),
    w: pos_integer(),

    # Key Value store of each node
    store: %{required(any()) => {[any()], %Context{}}},

    # Record of other alive nodes in the cluster
    alive_nodes: %{required(any()) => boolean()},

    # Hash ring for the dynamo cluster
    hash_ring: HashRing.t(),

    # Timeout for failing client request
    client_timeout: pos_integer(),

    # Timeout for redirecting request to coordinator 
    redirect_timeout: pos_integer(),

    # Timeout for syncing using merkle tree
    merkle_sync_timeout: pos_integer(),

    # Number of sconds a node should wait for a response
    request_timeout: pos_integer(),

    # Number of seconds after which the node checks if other nodes are alive
    health_check_timeout: pos_integer(),

    # For coordinator, the number of gets to wait for before responsing to client (R)
    gets_queue: %{
        required(pos_integer()) => %{
            client: any(),
            key: any(),
            responses: %{required(any()) => {[any()], %Context{}}},
            requested: MapSet.t(any())
        }
    },

    # For coordinator, the number of puts to wait for before responsing to client (W)
    puts_queue: %{
        required(pos_integer()) => %{
                client: any(),
                key: any(),
                value: any(),
                context: %Context{},
                responses: MapSet.t(any()),
                requested: %{required(any()) => any() | nil},
                last_requested_index: non_neg_integer()
            }
    },

    # Pending redirect requests to be forwarded to the coordinator
    redirect_queue: %{
        required(pos_integer) => %{
            client: any(),
            msg: %ClientGetRequest{} | %ClientPutRequest{},
            get_or_put: :get | :put
          }
    },

    # Pending handoffs to be forwarded to the appropriate node
    handoffs_queue: %{
      required(any()) => %{
        required(pos_integer()) => %{
          required(any()) => %Context{}
        }
      }
    }
  }

  @enforce_keys [
    :id,
    :n,
    :r,
    :w,
    :store,
    :nodes_alive,
    :ring,
    :client_timeout,
    :redirect_timeout,
    :merkle_sync_timeout, 
    :request_timeout,
    :health_check_timeout,
    :gets_queue, 
    :puts_queue,
    :redirect_queue,
    :handoffs_queue
]
  # The state of each node
  defstruct(
    :id,
    :n,
    :r,
    :w,
    :store,
    :nodes_alive,
    :ring,
    :client_timeout,
    :redirect_timeout,
    :merkle_sync_timeout, 
    :request_timeout,
    :health_check_timeout,
    :gets_queue, 
    :puts_queue,
    :redirect_queue,
    :handoffs_queue
  )