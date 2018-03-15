-module(test_app).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([]).

start() ->
  ok = application:start(testapp),
  {ok, ResultSetKey} = application:get_env(testapp, result_set_key),
  Client = redis:new_client(),
  {ok, _Response} = eredis:q(Client, ["DEL", ResultSetKey]),
  {Client, ResultSetKey}.

stop(_) ->
  application:stop(testapp).

prime_numbers_test_() ->
  {setup, fun start/0, fun stop/1, fun prime_numbers_appearing/1}.

prime_numbers_appearing({Client, ResultSetKey}) ->
  timer:sleep(2000),
  {ok, BinaryInt} = eredis:q(Client, ["SCARD", ResultSetKey]),
  PrimeNumberCount = list_to_integer(binary_to_list(BinaryInt)),
  {ok, RandomBinaryPrime} = eredis:q(Client, ["SRANDMEMBER", ResultSetKey]),
  PrimeNumber = list_to_integer(binary_to_list(RandomBinaryPrime)),
  [
    ?_assertNotEqual(PrimeNumberCount, 0),
    ?_assert(utils:is_prime(PrimeNumber))
  ].
