defmodule DatabaseThingTest do
  use ExUnit.Case
  doctest DatabaseThing

  test "greets the world" do
    assert DatabaseThing.hello() == :world
  end
end
