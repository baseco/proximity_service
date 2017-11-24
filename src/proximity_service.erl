-module(proximity_service).

-export([start/0, start/1]).
-export([get_env/1]).
-export([publish_event/3]).

-define(SQS_POOL_NAME, proximity_service_sqs_pool).
-define(SQS_POOL_SIZE, 10).

%%====================================================================
%% API
%%====================================================================

start() ->
	case get_env(service_handler) of
		undefined ->
			ok;
		Handler ->
			start(Handler)
    end.

start({_M, _F} = Handler) ->
	ok = start_sqs(Handler),
	ok.

get_env(Key) ->
	get_env(Key, undefined).

get_env(Key, Default) ->
	application:get_env(?MODULE, Key, Default).

publish_event(Topic, Event, Payload) when is_atom(Topic), is_binary(Event), is_map(Payload); is_binary(Payload) ->
	case topic_arn_by_topic(Topic) of
		{ok, TopicArn} ->
			EventData = event_data(Event, Payload),
			_ = erlcloud_sns:publish_to_topic(TopicArn, jiffy:encode(EventData), undefined),
			ok;
		error ->
			lager:error("Topic ~p not found", [Topic]),
			{error, not_found}
	end.

topic_arn_by_topic(Topic) ->
	Topics = get_env(topics, #{}),
	maps:find(Topic, Topics).

%%====================================================================
%% Internal functions
%%====================================================================

event_data(Event, Payload) ->
	System = #{<<"timestamp">> => unix_timestamp(), <<"publisher">> => atom_to_binary(node(), utf8)},
	#{<<"event">> => Event, <<"payload">> => Payload, <<"system">> => System}.

unix_timestamp() ->
    DateTime = calendar:universal_time(),
    calendar:datetime_to_gregorian_seconds(DateTime) - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).

start_sqs(Handler) ->
	ChildMods = [proximity_service_sqs],
    ChildMF = {proximity_service_sqs, start_link},
    _ = cuesport:start_link(?SQS_POOL_NAME, ?SQS_POOL_SIZE, ChildMods, ChildMF, {for_all, [Handler]}),
    ok.
