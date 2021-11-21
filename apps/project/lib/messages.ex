defmodule Context do
    alias __MODULE__

    @type t() :: %__MODULE__{
        version: VectorClock.t(),
        hint: any() | nil
    }
    
    @enforce_keys [:version]
    defstruct (
      :version,
      :hint
    )
  
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
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        key: any()
    }
    
    @enforce_keys [:nonce, :key]
    defstruct (
      :nonce,
      :key
    )

  end
  
  defmodule ClientGetResponse do
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        success: boolean(),
        values: [any()] | nil,
        context: %Context{} | nil
    }
    
    @enforce_keys [:nonce, :success, :values, :context]
    defstruct (
      :nonce,
      :success,
      :values,
      :context
    )
  end
  
  defmodule ClientPutRequest do
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        key: any(),
        value: any(),
        context: %Context{}
    }
    
    @enforce_keys [:nonce, :key, :value, :context]
    defstruct (
      :nonce,
      :key,
      :value,
      :context
    )   
    
  end
  
  defmodule ClientPutResponse do
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        success: boolean(),
        value: any(),
        context: %Context{}
    }
    
    @enforce_keys [:nonce, :success, :value, :context]
    defstruct (
      :nonce,
      :success,
      :value,
      :context
    )
  end

  defmodule CoordinatorGetRequest do
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        key: any()
    }
    
    @enforce_keys [:nonce, :key]
    defstruct (
      :nonce,
      :key
    )

  end
  
  defmodule CoordinatorGetResponse do
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        values: [any()] | nil,
        context: %Context{} | nil
    }
    
    @enforce_keys [:nonce, :values, :context]
    defstruct (
      :nonce,
      :values,
      :context
    )
  end
  
  defmodule CoordinatorPutRequest do
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        key: any(),
        value: any(),
        context: %Context{}
    }
    
    @enforce_keys [:nonce, :key, :value, :context]
    defstruct (
      :nonce,
      :key,
      :value,
      :context
    )   
    
  end
  
  defmodule CoordinatorPutResponse do
    alias __MODULE__

    @type t() :: %__MODULE__{
        nonce: pos_integer(),
        value: any(),
        context: %Context{}
    }
    
    @enforce_keys [:nonce, :value, :context]
    defstruct (
      :nonce,
      :value,
      :context
    )
  end
  