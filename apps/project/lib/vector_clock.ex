defmodule VectorClock do
    @moduledoc """
    Vector clocks for managing versions of keys.
    """
  
    @type t() :: %{required(any()) => non_neg_integer()}
  
    @doc """
    Return a new vector clock.
    """
    @spec new() :: t()
    def new() do
      %{}
    end
  
    @doc """
    Combine vector clocks: this is called whenever a
    message is received, and should return the clock
    from combining the two.
    """
    @spec combine(t(), t()) :: t()
    def combine(current, received) do
      Map.merge(current, received, fn _k, c, r -> max(c, r) end)
    end
  
    @doc """
    Increment the clock by one tick for a given process.
    """
    @spec tick(t(), any()) :: t()
    def tick(clock, proc) do
      Map.update(clock, proc, 1, &(&1 + 1))
    end
  
    @doc """
    Compare two vector clocks,
    returning one of {:before, :after, :concurrent}
    """
    @spec compare(t(), t()) :: :before | :after | :concurrent
    def compare(clock_1, clock_2) do
      keys =
        MapSet.union(MapSet.new(Map.keys(clock_1)), MapSet.new(Map.keys(clock_2)))
  
      comparisons =
        for key <- keys, into: MapSet.new() do
          val_1 = Map.get(clock_1, key, 0)
          val_2 = Map.get(clock_2, key, 0)
  
          cond do
            val_1 < val_2 -> :before
            val_1 > val_2 -> :after
            val_1 == val_2 -> :equal
          end
        end
  
      cond do
        Enum.empty?(comparisons) -> :concurrent

        :before in comparisons and :after in comparisons -> :concurrent

        :equal in comparisons and MapSet.size(comparisons) == 1 -> :concurrent

        :before in comparisons -> :before

        true -> :after
      end
    end
  
    @doc """
    Check if clock_1 is before clock_2.
    """
    @spec before?(t(), t()) :: boolean()
    def before?(clock_1, clock_2) do
      compare(clock_1, clock_2) == :before
    end
  
    @doc """
    Check if clock_1 is after clock_2.
    """
    @spec after?(t(), t()) :: boolean()
    def after?(clock_1, clock_2) do
      compare(clock_1, clock_2) == :after
    end
  
    @doc """
    Check if clock_1 is concurrent with clock_2.
    """
    @spec concurrent?(t(), t()) :: boolean()
    def concurrent?(clock_1, clock_2) do
      compare(clock_1, clock_2) == :concurrent
    end
  end
  