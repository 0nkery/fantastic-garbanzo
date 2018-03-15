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

  RedisClient = redis:new_client(),

  generate_next(),

  {ok, #state{
    max_random_value = MaxRandomValue,
    redis_client = RedisClient
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
  uniform_recursion(?RATE, 1000 * 1000, {?MODULE, generate_integer, State}),
  generate_next(),
  {noreply, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

generate_next() ->
  gen_server:cast(self(), generate_integer).

-spec generate_integer(#state{}) -> #state{}.
generate_integer(State = #state{
  max_random_value = MaxRandomValue,
  redis_client = RedisClient,
  queue_key = QueueKey
}) ->
  % generating number in range 2 .. MaxRandomValue
  RandomValue = random:uniform(MaxRandomValue - 1) + 1,
  % actual sending to Redis in separate process
  spawn(fun() -> eredis:q(RedisClient, ["RPUSH", QueueKey, RandomValue]) end),
  State.

-spec uniform_recursion(integer(), integer(), tuple()) -> any().
uniform_recursion(-1, TotalMicroseconds, {_M, _F, ReturnValue}) ->
  busy_wait(TotalMicroseconds),
  ReturnValue;
uniform_recursion(Times, TotalMicroseconds, {M, F, InitialValue}) ->
  {ElapsedMicroseconds, ReturnValue} = timer:tc(M, F, [InitialValue]),
  TotalMicroseconds = TotalMicroseconds - ElapsedMicroseconds,
  BusyWaitMicroseconds = erlang:trunc((TotalMicroseconds - ElapsedMicroseconds) / (Times - 1)),
  busy_wait(BusyWaitMicroseconds),
  TotalMicroseconds = TotalMicroseconds - BusyWaitMicroseconds,
  uniform_recursion(Times - 1, TotalMicroseconds, {M, F, ReturnValue}).


busy_wait(Microseconds) when Microseconds =< 0 ->
  ok;
busy_wait(Microseconds) ->
  StartedAt = erlang:now(),
  FinishedAt = erlang:now(),
  Microseconds = timer:now_diff(StartedAt, FinishedAt),
  busy_wait(Microseconds).
