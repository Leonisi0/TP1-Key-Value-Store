-module(main).
-export([start_link/2, stop/1, put/3, put/4, delete/2, get/2]).
-spec start_link(pos_integer(), pos_integer()) -> {ok, [pid()]}.
-spec stop([pid()]) -> ok.
-spec put(pid(), term(), term()) -> ok.
-spec put(pid(), term(), term(), non_neg_integer() | infinity) -> ok.
-spec delete(pid(), term()) -> ok.
-spec get(pid(), term()) -> {ok, term()} | not_found.

start_link(N, Alpha) when is_integer(N), N > 0, is_integer(Alpha), Alpha >= 3 -> 
    start_superLink(N, true, Alpha).

start_superLink(N, Primero, Alpha) when N >= 0 -> 
    ListaPids = case N of
        0 -> 
            [];
        _ ->
            Pid = spawn(fun() -> init(Alpha) end),    
            start_superLink(N-1, false, Alpha) ++ [Pid]
    end,
    case Primero of 
        true -> 
            todosConTodos(ListaPids, ListaPids),
            {ok, ListaPids};
        false ->
            ListaPids
    end.

todosConTodos(ListaPids, ListaEnviar) ->
    case ListaEnviar of 
        [] -> ok;
        _ -> 
            [Head | Tail ] = ListaEnviar,
            Head ! {configure, ListaPids},
            todosConTodos(ListaPids, Tail)
    end.
    
init(Alpha) ->
    receive 
        {configure, Pids} ->
            loop(#{}, Pids, Alpha)
    end.

stop(Pids) ->
    lists:foreach(fun(Pid) -> Pid ! {stop, self()} end, Pids),
    ok.
    
loop(State, ListaPids, Alpha) -> 
    receive 
        {put, Key, Value, TTL, SenderPid, ReferenceForSender} -> 
            NewState = put_handler(State, Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids), 
            loop(NewState, ListaPids, Alpha);
        {spread_put, Key, Value, TTL, Timestamp} -> 
            NewState = spread_put_handler(State, Alpha, Key, Value, TTL, Timestamp),
            loop(NewState, ListaPids, Alpha);
        {offer_key, OldestKey, OldestValue, OldestTTL, OldestTimestamp} -> 
            NewState = is_this_an_offer_i_can_refuse(State, Alpha, OldestKey, OldestValue, OldestTTL, OldestTimestamp),
            loop(NewState, ListaPids, Alpha);

        {get, Key, SenderPid, ReferenceForSender} ->  
            get_handler(State, ListaPids, Key, SenderPid, ReferenceForSender),
            loop(State, ListaPids, Alpha);
        {spread_get, Key, SenderPid, ReferenceForSender} ->
            spread_get_handler(State, Key, SenderPid, ReferenceForSender),
            loop(State, ListaPids, Alpha);

        {delete, Key, SenderPid, ReferenceForSender} ->
            NewState = delete_handler(State, Key, SenderPid, ReferenceForSender, ListaPids),
            loop(NewState, ListaPids, Alpha);

        {spread_delete, Key, Timestamp} ->
            NewState = spread_delete_handler(State, Key, Timestamp),
            loop(NewState, ListaPids, Alpha);

        {stop, SenderPid} -> 
            SenderPid ! ok
    end.

%%%%%%%%%% LLAMADOS %%%%%%%%%%%

put(Pid, Key, Value) -> 
    put(Pid, Key, Value, infinity).

put(Pid, Key, Value, TTL) when (is_integer(TTL) andalso TTL >= 0) orelse (TTL =:= infinity) -> 
    Ref = make_ref(),
    Pid ! {put, Key, Value, TTL, self(), Ref},
    receive 
        {ok, Ref} -> ok
    end.

get(Pid, Key) -> 
    Ref = make_ref(),
    Pid ! {get, Key, self(), Ref},         
    receive
        {Ref, Response} ->
            Response
    end.                                   

delete(Pid, Key) -> 
    Ref = make_ref(),
    Pid ! {delete, Key, self(), Ref},
    receive 
        {ok, Ref} -> 
            ok
    end.

%%%%%%%%%% FUNCIONES AUXILIARES %%%%%%%%%%

send_to_replicas(ToSend, Message, MyPid) ->
    case ToSend of 
        [] -> ok;
        _ -> 
            [Head | Tail ] = ToSend,
            if 
                Head == MyPid -> 
                    send_to_replicas(Tail, Message, MyPid);
                true ->
                    Head ! Message,
                    send_to_replicas(Tail, Message, MyPid)
            end
    end.

fork_and_seek(ToSend, Key, SenderPid, ReferenceForSender) ->
    Ref = make_ref(),
    MyPid = self(),
    spawn(fun() ->
        Message = {spread_get, Key, self(), Ref},
        send_to_replicas(ToSend, Message, MyPid),
        get_newest_value_or_nothing((length(ToSend)-1), none, SenderPid, Ref, ReferenceForSender)
    end).

get_newest_value_or_nothing(Iterator, NewestValue, SenderPid, MyRef, ReferenceForSender) ->
    case Iterator of
        0 ->
            case NewestValue of
                none ->
                    SenderPid ! {ReferenceForSender, not_found};
                {deleted,_,_} -> 
                    SenderPid ! {ReferenceForSender, not_found};
                {Value, _, _} ->
                    SenderPid ! {ReferenceForSender, {ok, Value}}
            end;
        _ ->
            receive 
                {MyRef, Value, TTL, Timestamp} ->
                    UpdatedValue = newest_unexpired_between(NewestValue, Value, TTL, Timestamp),
                    get_newest_value_or_nothing(Iterator-1, UpdatedValue, SenderPid, MyRef, ReferenceForSender);
                {MyRef, not_found} ->
                    get_newest_value_or_nothing(Iterator-1, NewestValue, SenderPid, MyRef, ReferenceForSender)
            end
    end.

newest_unexpired_between(NewestValue, deleted, TTL, Timestamp) ->
    case NewestValue of
        none ->
            {deleted, TTL, Timestamp};
        {_, _, NewestTimestamp} ->
            if
                Timestamp > NewestTimestamp ->
                    {deleted, TTL, Timestamp};
                true ->
                    NewestValue
            end
    end;

newest_unexpired_between(NewestValue, Value, TTL, Timestamp) ->
    case NewestValue of 
        none ->
            IsExpired = is_expired(TTL, Timestamp),
            if 
                IsExpired ->
                    none;
                true -> 
                    {Value, TTL, Timestamp}
            end;
        {_, _, NewestTimestamp} ->
            IsExpired = is_expired(TTL, Timestamp),
            if 
                IsExpired ->
                    NewestValue;
                true ->
                    if 
                        NewestTimestamp >= Timestamp ->
                            NewestValue;
                        true ->
                            {Value, TTL, Timestamp}
                    end
            end
    end.

is_this_an_offer_i_can_refuse(State, Alpha, OldestKey, OldestValue, OldestTTL, OldestTimestamp) ->
    FilteredState = filterExpiredValues(State),
    IsExpired = is_expired(OldestTTL, OldestTimestamp),
    case maps:find(OldestKey, FilteredState) of
        {ok, {_, _, Timestamp}} ->
            if
                OldestTimestamp > Timestamp ->
                    if
                        IsExpired ->
                            maps:remove(OldestKey, FilteredState);
                        true ->
                            FilteredState#{OldestKey => {OldestValue, OldestTTL, OldestTimestamp}}
                    end;
                true ->
                    FilteredState
            end;
        error ->
            if
                IsExpired ->
                    FilteredState;
                true ->
                    case maps:size(FilteredState) < Alpha of
                        true ->
                            FilteredState#{OldestKey => {OldestValue, OldestTTL, OldestTimestamp}};
                        false ->
                            FilteredState
                    end
            end
    end.
        
%%%%%%%%%% HANDLERS %%%%%%%%%%

is_expired(TTL, Timestamp) ->
    CurrentTime = erlang:system_time(millisecond),
    (TTL =/= infinity) andalso ((Timestamp + TTL) =< CurrentTime).

get_handler(State, ListaPids, Key, SenderPid, ReferenceForSender) ->
    case maps:find(Key, State) of
        {ok, {deleted, _, _}} ->
            SenderPid ! {ReferenceForSender, not_found};
        {ok, {Value, TTL, Timestamp}} ->
            IsExpired = is_expired(TTL, Timestamp),
            if 
                IsExpired ->
                    fork_and_seek(ListaPids, Key, SenderPid, ReferenceForSender);
                true ->
                    SenderPid ! {ReferenceForSender, {ok, Value}} 
            end;
        error ->
            fork_and_seek(ListaPids, Key, SenderPid, ReferenceForSender)
    end.

spread_get_handler(State, Key, SenderPid, ReferenceForSender) ->
    case maps:find(Key, State) of 
        {ok, {deleted, TTL, Timestamp}} ->
            SenderPid ! {ReferenceForSender, deleted, TTL, Timestamp};
        {ok, {Value, TTL, Timestamp}} ->
            IsExpired = is_expired(TTL, Timestamp),
            if 
                IsExpired -> 
                    SenderPid ! {ReferenceForSender, not_found};
                true ->
                    SenderPid ! {ReferenceForSender, Value, TTL, Timestamp}
            end;
        error ->
            SenderPid ! {ReferenceForSender, not_found}
    end.

put_handler(State, Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids) ->
    case maps:find(Key, State) of 
        {ok, {_, _, _}} -> 
            put_and_spread(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids);
        error -> 
            FreeSpace = Alpha - maps:size(State),
            case FreeSpace of
                0 ->
                    filter_expired_or_deallocate_oldest(State, Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids);            
                _ -> 
                    put_and_spread(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids)
            end
    end.

put_and_spread(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids) ->
    Timestamp = erlang:system_time(millisecond),
    NewState = State#{Key => {Value, TTL, Timestamp}},
    SenderPid ! {ok, ReferenceForSender},
    Message = {spread_put, Key, Value, TTL, Timestamp},         
    send_to_replicas(ListaPids, Message, self()),
    NewState.

filterExpiredValues(State) ->
    maps:filter(
        fun(_, {_, TTL, Timestamp}) ->
            not is_expired(TTL, Timestamp)
        end,
        State
    ).

filter_expired_or_deallocate_oldest(State, Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids) ->
    FilteredState = filterExpiredValues(State),
    FreeSpace = Alpha - maps:size(FilteredState),
    case FreeSpace of 
        0 -> 
            deallocate_oldest_and_offer_to_other_replicas(State, Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids);
        _ -> 
            put_and_spread(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids)
    end.

deallocate_oldest_and_offer_to_other_replicas(State, _Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids) ->
    Oldest = maps:fold(
        fun(K, {V, T, Ts}, OldestSoFar) ->
            case OldestSoFar of
                none ->
                    {K, V, T, Ts};
                {_, _, _, OldTimestamp} when Ts < OldTimestamp ->
                    {K, V, T, Ts};
                _ ->
                    OldestSoFar
            end
        end,
        none,
        State
    ),
    {OldestKey, OldestValue, OldestTTL, OldestTimestamp} = Oldest,
    spawn(fun() ->
        Message = {offer_key, OldestKey, OldestValue, OldestTTL, OldestTimestamp},
        send_to_replicas(ListaPids, Message, self())
    end),
    StateWithFreeSpace = maps:remove(OldestKey, State),
    put_and_spread(StateWithFreeSpace, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids).

spread_put_handler(State, _Alpha, Key, Value, TTL, Timestamp) ->
    case maps:find(Key, State) of
        {ok, {MyValue, _, MyTimestamp}} -> 
            CurrentTime = erlang:system_time(millisecond),
            %% Desempate determinista si los timestamps son iguales:
            IsStrictlyNewer = (Timestamp > MyTimestamp) orelse 
                              ((Timestamp == MyTimestamp) andalso (Value > MyValue)),
            IsUnexpired = if 
                TTL =:= infinity -> true;
                true -> (Timestamp + TTL) > CurrentTime
            end,
            if 
                IsStrictlyNewer andalso IsUnexpired ->
                    State#{Key => {Value, TTL, Timestamp}};
                true ->
                    State
            end;
        error -> 
            CurrentTime = erlang:system_time(millisecond),
            IsExpired = (TTL =/= infinity) andalso ((Timestamp + TTL) =< CurrentTime),
            if 
                IsExpired =:= false ->
                    State#{Key => {Value, TTL, Timestamp}};
                true ->
                    State
            end
    end.

delete_handler(State, Key, SenderPid, ReferenceForSender, ListaPids) -> 
    Now = erlang:system_time(millisecond),
    NewState = State#{Key => {deleted, infinity, Now}},
    SenderPid ! {ok, ReferenceForSender},
    send_to_replicas(ListaPids, {spread_delete, Key, Now}, self()),
    NewState.

spread_delete_handler(State, Key, DeleteTimestamp) ->
    case maps:find(Key, State) of
        {ok, {_, _, LocalTimestamp}} ->
            if 
                DeleteTimestamp >= LocalTimestamp ->
                    State#{Key => {deleted, infinity, DeleteTimestamp}};
                true ->
                    State 
            end;
        error ->
            State#{Key => {deleted, infinity, DeleteTimestamp}}
    end.