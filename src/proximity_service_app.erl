%%%-------------------------------------------------------------------
%% @doc proximity_service public API
%% @end
%%%-------------------------------------------------------------------

-module(proximity_service_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    proximity_service_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================