defmodule VectorClockTest do
    use ExUnit.Case
    doctest VectorClock
  
    test "combine_vector_clocks is correct" do
      assert VectorClock.combine(
               %{a: 6, b: 2, c: 6},
               %{a: 1, b: 200, c: 6}
             ) == %{a: 6, b: 200, c: 6}
  
      assert VectorClock.combine(%{a: 2}, %{b: 3}) ==
               %{a: 2, b: 3}
    end
  
    test "update_vector_clock is correct" do
      assert VectorClock.tick(%{a: 7, b: 22}, :a) ==
               %{a: 8, b: 22}
    end
  
    test "compare_vectors is correct" do
      assert VectorClock.compare(%{a: 8, b: 6}, %{a: 7, b: 5}) == :after
      assert VectorClock.compare(%{a: 7, b: 5}, %{a: 8, b: 6}) == :before
      assert VectorClock.compare(%{a: 7, b: 5}, %{a: 7, b: 5}) == :concurrent
      assert VectorClock.compare(%{a: 1, b: 2}, %{a: 2, b: 1}) == :concurrent
      assert VectorClock.compare(%{a: 22}, %{b: 66}) == :concurrent
    end
  end