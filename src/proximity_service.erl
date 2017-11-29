-module(proximity_service).

-export([start/0, start/1]).
-export([get_env/1]).
-export([publish_event/3]).
-export([publish_event_ex/4]).
-export([get_ex_data/1]).
-export([del_ex_data/1]).

-define(SQS_POOL_NAME, proximity_service_sqs_pool).
-define(SQS_POOL_SIZE, 10).
-define(REDIS_HOST, "127.0.0.1").
-define(REDIS_PORT, 6379).
-define(REDIS_POOL_NAME, proximity_service_redis_pool).
-define(REDIS_POOL_SIZE, 15).
-define(REDIS_EXPIRE, 3 * 60 * 60).
-define(DEFAULT_COWBOY_PORT, 8763).
-define(ATTEMPTS, 3).

%%====================================================================
%% API
%%====================================================================

start() ->
    ok = start_redis(),
	case get_env(service_handler) of
		undefined ->
			ok;
		Handler ->
			start(Handler)
    end.

start({_M, _F} = Handler) ->
	ok = start_cowboy_and_maybe_sqs(Handler),
	ok.

get_env(Key) ->
	get_env(Key, undefined).

get_env(Key, Default) ->
	application:get_env(?MODULE, Key, Default).

publish_event(Topic, Event, Payload) when is_atom(Topic), is_binary(Event), is_map(Payload) ->
	EventData = event_data(Event, Payload),
	publish_to_topic(Topic, EventData).

publish_event_ex(Topic, Event, Payload, ExData) when is_atom(Topic), is_binary(Event), is_map(Payload), is_binary(ExData) ->
	ExDataId = gen_id(),
	ok = set_ex_data(ExDataId, ExData),
	EventData = event_data(Event, Payload, #{<<"ex_data_id">> => ExDataId}),
	publish_to_topic(Topic, EventData).

topic_arn_by_topic(Topic) ->
	Topics = get_env(topics, #{}),
	maps:find(Topic, Topics).

get_ex_data(#{<<"system">> := #{<<"ex_data_id">> := ExDataId}}) ->
    case redis_q(["GET", ExDataId]) of
        {ok, undefined} ->
            {error, not_found};
        {error, query_fail} ->
            {error, not_found};
        {ok, Value} ->
            {ok, Value}
    end.

del_ex_data(#{<<"system">> := #{<<"ex_data_id">> := ExDataId}}) ->
	case redis_q(["DEL", ExDataId]) of
        {ok, _} ->
            ok;
        {error, query_fail} ->
            ok
    end.

%%====================================================================
%% Internal functions
%%====================================================================

publish_to_topic(Topic, EventData) ->
    case topic_arn_by_topic(Topic) of
        {ok, TopicArn} ->
            publish_to_topic(TopicArn, EventData, ?ATTEMPTS);
        error ->
            {error, not_found}
    end.

publish_to_topic(_TopicArn, EventData, Attempt) when Attempt == 0 ->
    lager:error("Failed publish event ~p", [EventData]),
    error;
publish_to_topic(TopicArn, EventData, Attempt) when Attempt > 0 ->
    try
        _ = erlcloud_sns:publish_to_topic(TopicArn, jiffy:encode(EventData), undefined),
        ok
    catch
        error:{sns_error, _} = Error ->
            lager:warning("SNS error ~p, attempt ~p, try again", [Attempt, Error]),
            publish_to_topic(TopicArn, EventData, Attempt - 1)
    end.

event_data(Event, Payload) ->
	event_data(Event, Payload, #{}).

event_data(Event, Payload, System) ->
	NewSystem = System#{<<"timestamp">> => unix_timestamp(), <<"publisher">> => atom_to_binary(node(), utf8)},
	#{<<"event">> => Event, <<"payload">> => Payload, <<"system">> => NewSystem}.

unix_timestamp() ->
    DateTime = calendar:universal_time(),
    calendar:datetime_to_gregorian_seconds(DateTime) - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).

start_cowboy_and_maybe_sqs(Handler) ->
    ok = start_cowboy(Handler),
    case get_env(need_sqs, false) of
        false ->
            ok;
        true ->
            start_sqs(Handler)
    end.

start_cowboy(Handler) ->
	Port = get_env(service_cowboy_port, ?DEFAULT_COWBOY_PORT),
	proximity_service_cowboy:start(Handler, Port).

start_sqs(Handler) ->
	ChildMods = [proximity_service_sqs],
    ChildMF = {proximity_service_sqs, start_link},
    _ = cuesport:start_link(?SQS_POOL_NAME, ?SQS_POOL_SIZE, ChildMods, ChildMF, {for_all, [Handler]}),
    ok.

start_redis() ->
	ChildMods = [eredis],
    ChildMF = {eredis, start_link},
    RedisHost = os:getenv("REDIS_HOST", ?REDIS_HOST),
    _ = cuesport:start_link(?REDIS_POOL_NAME, ?REDIS_POOL_SIZE, ChildMods, ChildMF, {for_all, [RedisHost, ?REDIS_PORT]}),
    ok.

set_ex_data(ExDataId, ExData) ->
	case redis_q(["SETEX", ExDataId, ?REDIS_EXPIRE, ExData]) of
        {ok, <<"OK">>} ->
            ok;
        {error, query_fail} ->
            ok
    end.

gen_id() ->
	list_to_binary(uuid:uuid_to_string(uuid:get_v4())).

redis_q(Query) ->
	try eredis:q(get_redis_worker(), Query) of
        {error, _Reason} = Error ->
            lager:error("Failed run redis query ~p ~p", [Query, Error]),
            {error, query_fail};
        Ret ->
            Ret
    catch
        C:R ->
            ST = erlang:get_stacktrace(),
            lager:warning("Failed run redis query ~p ~p ~p", [Query, {C, R}, ST]),
            {error, query_fail}
    end.

get_redis_worker() ->
    cuesport:get_worker(?REDIS_POOL_NAME).
