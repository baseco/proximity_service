-module(proximity_service_cowboy).

-export([init/2]).
-export([start/2]).

%%====================================================================
%% API
%%====================================================================

start(Handler, Port) ->
	Dispatch = cowboy_router:compile([
		{'_', [
			{"/", ?MODULE, [Handler]}
		]}
	]),
	{ok, _} = cowboy:start_clear(http, [{port, Port}], #{env => #{dispatch => Dispatch}}),
	ok.

%%====================================================================
%% Cowboy callbacks
%%====================================================================

init(Req, Opts) ->
	RetReq = handle(Req, Opts),
	{ok, RetReq, Opts}.

%%====================================================================
%% Internal functions
%%====================================================================

handle(Req, [ServiceHandler]) ->
	{ok, Body, Req2} = cowboy_req:read_body(Req),
	lager:info("Body: ~p", [Body]),
	RetReq = cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, <<"Hello world!">>, Req2),
	RetReq.