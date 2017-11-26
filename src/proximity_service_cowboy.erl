-module(proximity_service_cowboy).

-export([init/2]).
-export([start/2]).

%%====================================================================
%% API
%%====================================================================

start(Handler, Port) ->
	Dispatch = cowboy_router:compile([
		{'_', [
			{"/event", ?MODULE, [Handler]},
			{"/ping", ?MODULE, []}
		]}
	]),
	{ok, _} = cowboy:start_clear(http, [{port, Port}], #{env => #{dispatch => Dispatch}}),
	ok.

%%====================================================================
%% Cowboy callbacks
%%====================================================================

init(Req, Opts) ->
	Path = cowboy_req:path(Req),
	RetReq = handle(Path, Req, Opts),
	{ok, RetReq, Opts}.

%%====================================================================
%% Internal functions
%%====================================================================

handle(<<"/ping">>, Req, _Opts) ->
    %% TODO: add healthcheck
	cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, <<"pong">>, Req);
handle(<<"/event">>, Req, [Handler]) ->
    handle_event(Req, Handler).

handle_event(Req, {Module, Function}) ->
	{ok, Body, Req2} = cowboy_req:read_body(Req),
    EventData = get_event_data(Body),
	try
        Module:Function(EventData),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, <<"OK">>, Req2)
    catch
        C:R ->
            ST = erlang:get_stacktrace(),
            lager:error("Failed handle event data ~p error ~p ~p", [EventData, {C, R}, ST]),
            %% Now return also 200 response on fail for delete messages from SQS
            cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, <<"OK">>, Req2)
    end.

get_event_data(Body) when is_binary(Body) ->
    Map = jiffy:decode(Body, [return_maps]),
    jiffy:decode(maps:get(<<"Message">>, Map), [return_maps]).
