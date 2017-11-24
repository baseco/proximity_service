-module(proximity_service_sqs).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tref :: reference(), handler :: {atom(), atom()}}).

-define(TIMEOUT, 1000).
-define(MAX_MESSAGES, 3).
-define(WAIT_TIME_SECONDS, 20).

%%====================================================================
%% API
%%====================================================================

start_link(Handler) ->
    gen_server:start_link(?MODULE, [Handler], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Handler]) ->
    ok = proximity_service:maybe_configure_aws(),
    State = timer_up(#state{}),
    {ok, State#state{handler = Handler}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(receive_messages, State) ->
    ok = handle_receive_messages(State#state.handler),
    NewState = timer_up(State),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

handle_receive_messages(Handler) ->
    ServiceQueue = proximity_service:get_env(service_queue),
    try erlcloud_sqs:receive_message(ServiceQueue, [], ?MAX_MESSAGES, ?WAIT_TIME_SECONDS) of
    	[{messages, Messages}] ->
    		ok = handle_messages(Messages, Handler)
    catch
    	error:{aws_error, _Reason} ->
            ok;
        C:R ->
            ST = erlang:get_stacktrace(),
            lager:error("Failed handle SQS messages ~p ~p", [{C, R}, ST]),
            ok
    end.

handle_messages(Messages, Handler) ->
    lists:foreach(fun(Message) -> handle_message(Message, Handler) end, Messages).

handle_message(Message, Handler) ->
    Body = proplists:get_value(body, Message),
    try 
        case get_event_data(Body) of
            EventData when is_map(EventData) ->
                try_handle_event(EventData, Handler);
            Other ->
                lager:error("Decode body returned unknown payload ~p", [Other])
        end
    catch
        C:R ->
            ST = erlang:get_stacktrace(),
            lager:error("Failed decode body and message will be delete ~p ~p ~p", [Body, {C, R}, ST])
    after
        ok = try_delete(Message)
    end.

try_handle_event(EventData, {Module, Function}) ->
    Module:Function(EventData).

get_event_data(Body) when is_list(Body) ->
    Map = jiffy:decode(list_to_binary(Body), [return_maps]),
    jiffy:decode(maps:get(<<"Message">>, Map), [return_maps]).

timer_up(#state{tref = TRef} = State) ->
    catch erlang:cancel_timer(TRef),
    NewTRef = erlang:send_after(?TIMEOUT, self(), receive_messages),
    State#state{tref = NewTRef}.

try_delete(Message) ->
    try
        ServiceQueue = proximity_service:get_env(service_queue),
        ReceiptHandle = proplists:get_value(receipt_handle, Message),
        erlcloud_sqs:delete_message(ServiceQueue, ReceiptHandle)
    catch
        C:R ->
            lager:error("Failed delete message ~p from SQS ~p", [Message, {C, R}])
    end.
