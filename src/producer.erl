-module(producer).

-behaviour(gen_server).

%% API
-export([start_link/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% gen_server callbacks
-export([init/1,
  handle_cast/2
]).

-define(SERVER, ?MODULE).
-define(RATE, 3).

-record(state, {redis_client, queue_key, max_random_value}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  {ok, MaxRandomValue} = application:get_env(testapp, max_random_value),
  {ok, QueueKey} = application:get_env(testapp, queue_key),

  RedisClient = redis:new_client(),

  generate_next(),

  {ok, #state{
    max_random_value = MaxRandomValue,
    redis_client = RedisClient,
    queue_key = QueueKey
  }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: generate_integer, State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(generate_integer, State) ->
  run_n_times_uniformly(?RATE, 1000 * 1000, fun() -> generate_integer(State) end),
  generate_next(),
  {noreply, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Schedules next integer generation op.
generate_next() ->
  gen_server:cast(self(), generate_integer).

% Generates uniform distributed integer and stores
% it to Redis queue.
-spec generate_integer(#state{}) -> #state{}.
generate_integer(#state{
  max_random_value = MaxRandomValue,
  redis_client = RedisClient,
  queue_key = QueueKey
}) ->
  % generating number in range 2 .. MaxRandomValue
  RandomValue = random:uniform(MaxRandomValue - 1) + 1,
  % actual sending to Redis in separate process
  spawn(fun() -> eredis:q(RedisClient, ["RPUSH", QueueKey, RandomValue]) end).

% Uniformly calls given fn N times in given time interval on
% best-effort basis (ideal uniformity is not guaranteed).
-spec run_n_times_uniformly(integer(), integer(), tuple()) -> any().
run_n_times_uniformly(1, TotalMicroseconds, Fun) ->
  {ElapsedMicroseconds, _ReturnValue} = timer:tc(Fun),
  MicrosecondsLeft = TotalMicroseconds - ElapsedMicroseconds,
  busy_wait(MicrosecondsLeft);
run_n_times_uniformly(Times, TotalMicroseconds, Fun) ->
  {ElapsedMicroseconds, _ReturnValue} = timer:tc(Fun),
  MicrosecondsLeft = TotalMicroseconds - ElapsedMicroseconds,
  BusyWaitMicroseconds = erlang:trunc((MicrosecondsLeft - (Times - 1) * ElapsedMicroseconds) / (Times - 1)),
  busy_wait(BusyWaitMicroseconds),
  NewTotalMicroseconds = MicrosecondsLeft - BusyWaitMicroseconds,
  run_n_times_uniformly(Times - 1, NewTotalMicroseconds, Fun).

% Busy waits during given amount of time.
busy_wait(Microseconds) when Microseconds =< 0 ->
  ok;
busy_wait(Microseconds) ->
  StartedAt = erlang:now(),
  FinishedAt = erlang:now(),
  Elapsed = timer:now_diff(FinishedAt, StartedAt),
  % this fn takes twice as much time without '- 1' here
  % since Elapsed is almost always is equal to 1 we subtracting
  % time for other ops and in the end we're almost precise
  NewMicroseconds = Microseconds - Elapsed - 1,
  busy_wait(NewMicroseconds).

-ifdef(TEST).

busy_wait_test() ->
  BusyWaitMicroseconds = [100, 300, 50, 30000, 5000],
  Precision = 2,
  lists:foreach(fun(M) ->
    StartedAt = erlang:now(),
    busy_wait(M),
    Elapsed = timer:now_diff(erlang:now(), StartedAt),
    ?assert(erlang:abs(Elapsed - M) < Precision)
  end, BusyWaitMicroseconds).

run_n_times_uniformly_test() ->
  Tests = [{3, 1000 * 1000}, {2, 1000}, {5, 1000 * 100}],
  Precision = 5,
  TestFun = fun({Times, M}) ->
    StartedAt = erlang:now(),
    Self = self(),
    ValueToReceive = called,
    run_n_times_uniformly(Times, M, fun() -> Self ! ValueToReceive end),
    receive_n_times(Times, ValueToReceive),
    Elapsed = timer:now_diff(erlang:now(), StartedAt),
    io:format("elapsed ~p - should be ~p ~n", [Elapsed, M]),
    ?assert(erlang:abs(Elapsed - M) < Precision)
  end,
  lists:foreach(fun(T) -> lists:duplicate(100, TestFun(T)) end, Tests).

receive_n_times(0, _ValueToReceive) ->
  ok;
receive_n_times(N, ValueToReceive) ->
  receive
    ValueToReceive ->
      receive_n_times(N - 1, ValueToReceive)
  end.

-endif.