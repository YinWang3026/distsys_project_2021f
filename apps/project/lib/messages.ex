defmodule Context do
    @typedoc """
    Metadata for get/put messages, contains version and hint info.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        version: VectorClock.t(),
        hint: any() | nil
    }
    
    @enforce_keys [:version]
    defstruct([
      :version,
      :hint
    ])
  
    def compare(ctx1, ctx2) do
      VectorClock.compare(ctx1.version, ctx2.version)
    end
  
    def combine(ctx1, ctx2) do
      case compare(ctx1, ctx2) do
        :before ->
          ctx2
  
        :after ->
          ctx1
  
        :concurrent ->
          %Context{
            version: VectorClock.combine(ctx1.version, ctx2.version),
            hint:
              if ctx1.hint != nil do
                ctx1.hint
              else
                ctx2.hint
              end
          }
      end
    end
  end
  
  defmodule ClientGetRequest do
    @typedoc """
    Get request sent by client.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        key: any()
    }
    
    @enforce_keys [:nonce, :key]
    defstruct([
      :nonce,
      :key
    ])

  end
  
  defmodule ClientGetResponse do
    @typedoc """
    Response to get request received by client
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        success: boolean(),
        values: [any()] | nil,
        context: %Context{} | nil
    }
    
    @enforce_keys [:nonce, :success, :values, :context]
    defstruct([
      :nonce,
      :success,
      :values,
      :context
    ])
  end
  
  defmodule ClientPutRequest do
    @typedoc """
    Put request sent by client.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        key: any(),
        value: any(),
        context: %Context{}
    }
    
    @enforce_keys [:nonce, :key, :value, :context]
    defstruct([
      :nonce,
      :key,
      :value,
      :context
    ])   
    
  end
  
  defmodule ClientPutResponse do
    @typedoc """
    Response to put request received by client.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        success: boolean(),
        value: any(),
        context: %Context{}
    }
    
    @enforce_keys [:nonce, :success, :value, :context]
    defstruct([
      :nonce,
      :success,
      :value,
      :context
    ])
  end

  defmodule CoordinatorGetRequest do
    @typedoc """
    Get request sent by coordinator to a dynamo node.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        key: any()
    }
    
    @enforce_keys [:nonce, :key]
    defstruct([
      :nonce,
      :key
    ])

  end
  
  defmodule CoordinatorGetResponse do
    @typedoc """
    Response to get request sent by coordinator.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        values: [any()] | nil,
        context: %Context{} | nil
    }
    
    @enforce_keys [:nonce, :values, :context]
    defstruct([
      :nonce,
      :values,
      :context
    ])
  end
  
  defmodule CoordinatorPutRequest do
    @typedoc """
    Put request sent by coordinator to a dynamo node.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        key: any(),
        value: any(),
        context: %Context{}
    }
    
    @enforce_keys [:nonce, :key, :value, :context]
    defstruct([
      :nonce,
      :key,
      :value,
      :context
    ])   
    
  end
  
  defmodule CoordinatorPutResponse do
    @typedoc """
    Response to put request sent by coordinator.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer()
    }
    
    @enforce_keys [:nonce]
    defstruct([
      :nonce
    ])
  end

  defmodule GetStateRequest do
    @typedoc """
    Request to get the current state of a node, used for testing.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
    }
    
    @enforce_keys [:nonce]
    defstruct([
      :nonce
    ])
  end

  defmodule GetStateResponse do
    @typedoc """
    Returns the current state of node, used for testing.
    """
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        state: %DynamoNode{}
    }
    
    @enforce_keys [:nonce, :state]
    defstruct([
      :nonce,
      :state
    ])
  end

  defmodule RedirectedClientRequest do
    @typedoc """
    Returns the client request if current node is not the coordinator.
    """

    alias __MODULE__

    @type t() :: %__MODULE__{
        client: any(),
        request: %ClientGetRequest{} | %ClientPutRequest{}
    }
    
    @enforce_keys [:client, :request]
    defstruct([
      :client,
      :request
    ])
  end

  defmodule RedirectAcknowledgement do
    @typedoc """
      Acknowledgement for a redirect.
    """
    alias __MODULE__
    @type t() :: %__MODULE__{
      nonce: pos_integer()
    }
  
    @enforce_keys [:nonce]
    defstruct([
      :nonce
    ])
  end

  defmodule HandoffRequest do
    @typedoc """
    Handoff hinted data to original owner.
    """

    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        data: %{required(any()) => {[any()], %Context{}}}
    }
    
    @enforce_keys [:nonce, :data]
    defstruct([
      :nonce,
      :data
    ])
  end
  
  defmodule HandoffResponse do
    @typedoc """
      Handoff request ack.
    """

    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
    }
    
    @enforce_keys [:nonce]
    defstruct([
      :nonce
    ])
  end
  