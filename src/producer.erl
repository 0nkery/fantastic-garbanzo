-module(producer).

-behaviour(gen_server).

%% API
-export([start_link/0]).

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
  uniform_recursion(?RATE, 1000 * 1000, fun() -> generate_integer(State) end),
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
-spec uniform_recursion(integer(), integer(), tuple()) -> any().
uniform_recursion(1, TotalMicroseconds, Fun) ->
  {ElapsedMicroseconds, _ReturnValue} = timer:tc(Fun),
  MicrosecondsLeft = TotalMicroseconds - ElapsedMicroseconds,
  busy_wait(MicrosecondsLeft);
uniform_recursion(Times, TotalMicroseconds, Fun) ->
  {ElapsedMicroseconds, _ReturnValue} = timer:tc(Fun),
  MicrosecondsLeft = TotalMicroseconds - ElapsedMicroseconds,
  BusyWaitMicroseconds = erlang:trunc((MicrosecondsLeft - (Times - 1) * ElapsedMicroseconds) / (Times - 1)),
  busy_wait(BusyWaitMicroseconds),
  NewTotalMicroseconds = MicrosecondsLeft - BusyWaitMicroseconds,
  uniform_recursion(Times - 1, NewTotalMicroseconds, Fun).

% Busy waits during given amount of time.
busy_wait(Microseconds) when Microseconds =< 0 ->
  ok;
busy_wait(Microseconds) ->
  StartedAt = erlang:now(),
  FinishedAt = erlang:now(),
  Elapsed = timer:now_diff(FinishedAt, StartedAt),
  NewMicroseconds = Microseconds - Elapsed,
  busy_wait(NewMicroseconds).
