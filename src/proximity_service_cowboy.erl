-module(proximity_service_cowboy).

-export([init/2]).
-export([start/2]).

%%====================================================================
%% API
%%====================================================================

start(Handler, Port) ->
	Dispatch = cowboy_router:compile([
		{'_', [
			{"/", ?MODULE, [Handler]},
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
	cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, <<"pong">>, Req);
handle(<<"/">>, Req, Opts) ->
	{ok, Body, Req2} = cowboy_req:read_body(Req),
	lager:info("Opts: ~p Body: ~p", [Opts, Body]),
	RetReq = cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, <<"Hello world!">>, Req2),
	RetReq.