-module(redis).

%% API
-export([new_client/0]).


-spec new_client() -> eredis:client().
new_client() ->
  {ok, RedisHost} = application:get_env(testapp, redis_host),
  {ok, RedisPort} = application:get_env(testapp, redis_port),
  {ok, RedisDB} = application:get_env(testapp, redis_db),
  {ok, RedisClient} = eredis:start_link(RedisHost, RedisPort, RedisDB),
  RedisClient.
