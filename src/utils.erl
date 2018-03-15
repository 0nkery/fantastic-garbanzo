-module(utils).

%% API
-export([is_prime/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% Checks if given integer is prime number.
-spec is_prime(integer()) -> boolean().
is_prime(N) ->
  is_prime(N, 2, erlang:trunc(math:sqrt(N) + 1)).

-spec is_prime(integer(), integer(), integer()) -> boolean().
is_prime(_, Max, Max) ->
  true;
is_prime(N, I, Max) ->
  if
    N rem I =:= 0 ->
      false;
    true ->
      is_prime(N, I + 1, Max)
  end.


-ifdef(TEST).

is_prime_test() ->
  Tests = [{2, true}, {3, true}, {4, false}, {11, true}, {20, false}, {997, true}],
  lists:foreach(fun({N, ShouldBe}) ->
    ?assertEqual(is_prime(N), ShouldBe)
                end, Tests).

-endif.