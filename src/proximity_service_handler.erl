-module(proximity_service_handler).

-export([handle/1]).

%%====================================================================
%% API
%%====================================================================

handle(Payload) ->
	lager:info("Handle ~p", [Payload]),
	ok.