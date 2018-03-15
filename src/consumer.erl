-module(consumer).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([
  init/1,
  handle_cast/2
]).

-define(SERVER, ?MODULE).

-record(state, {redis_client, queue_key, result_set_key}).

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
  RedisClient = redis:new_client(),

  {ok, QueueKey} = application:get_env(testapp, queue_key),
  {ok, ResultSetKey} = application:get_env(testapp, result_set_key),

  get_next_integer(),

  {ok, #state{
    redis_client = RedisClient,
    queue_key = QueueKey,
    result_set_key = ResultSetKey
  }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: get_next_integer, State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(get_next_integer, State) ->
  State = process_integer(State),
  get_next_integer(),
  {noreply, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Schedules next integer processing.
get_next_integer() ->
  gen_server:cast(self(), get_next_integer).

% Determines if the next integer from Redis queue is prime
% number. If it is, stores it in Redis set.
-spec process_integer(#state{}) -> #state{}.
process_integer(State = #state{
  queue_key = QueueKey,
  result_set_key = ResultSetKey,
  redis_client = RedisClient}
) ->
  {ok, [_List, BinaryInt]} = eredis:q(RedisClient, ["BLPOP", QueueKey, 0], infinity),
  Integer = list_to_integer(binary_to_list(BinaryInt)),
  case utils:is_prime(Integer) of
    true ->
      eredis:q(RedisClient, ["SADD", ResultSetKey, Integer]);
    _Else ->
      false
  end,
  State.
