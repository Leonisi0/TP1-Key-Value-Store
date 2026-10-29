-module(simple_kv).
-export([start_link/1, stop/1, put/3, put/4, delete/2, get/2]).
-spec start_link(pos_integer()) -> {ok, [pid()]}.
-spec stop([pid()]) -> ok.
-spec put(pid(), term(), term()) -> ok.
-spec put(pid(), term(), term(), non_neg_integer() | infinity) -> ok.
-spec delete(pid(), term()) -> ok.
-spec get(pid(), term()) -> {ok, term()} | not_found.

start_link(N, Alpha) when N >= 0 -> 
    start_superLink(N, true, Alpha).

start_superLink(N, Primero, Alpha) when N >= 0 -> 
    ListaPids = case N of
        0 -> 
            [];
        _ ->
            Pid = spawn(fun init/0),    
            start_superLink(N-1, false, Alpha) ++ [Pid]
    end,
    case Primero of 
        true -> 
            todosConTodos(ListaPids, length(ListaPids), Alpha),
            {ok, ListaPids};
        false ->
            ListaPids
    end.

todosConTodos(ListaPids, ListaEnviar) ->
    case ListaEnviar of 
        [] -> ok;
        _ -> 
            [Head | Tail ] = ListaEnviar,
            Head ! ListaPids,
            todosConTodos(ListaPids, Tail)
    end.
    
init() ->
    receive 
        {configure, Pids} ->
            loop(#{}, Pids)
    end.
    
loop(State, ListaPids, Alpha) -> 
    receive 
        {put, Key, Value, TTL, SenderPid, ReferenceForSender} -> 
            NewState = put_handler(State, Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids), 
            loop(NewState, ListaPids, Alpha);
        {spread_put, Key, Value, TTL, Timestamp} -> 
            NewState = spread_put_handler(State, Alpha, Key, Value, TTL, Timestamp),
            loop(NewState, ListaPids, Alpha);
        {offer_key, OldestKey, OldestValue, OldestTTL, OldetsTimestamp} -> 
            NewState = is_this_an_offer_i_can_refuse(State, Alpha, OldestKey, OldestValue, OldestTTL, OldetsTimestamp),
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

put(Pid, Key, Value, TTL) -> 
    Ref = make_ref(),
    Pid ! {put, Key, Value, TTL, self(), Ref},
    receive 
        {ok, Ref} -> ok %ok
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

fork_and_seek(ToSend, Key, SenderPid, ReferenceForSender) -> %hay que ver que argumentos me paso
    Ref = make_ref(),
    MyPid = self(), %guardamos nuestro pid antes del fork para que no mire su pid sino el del padre (nosotros)
    spawn(fun() ->
        Message = {spread_get, Key, self(), Ref}, %aca adentro le pasamos nuestro pid osea el del fork
        send_to_replicas(ToSend, Message, MyPid), %con esta me pongo a preguntarle a todos, no se si puedo usar esta
        get_newest_value_or_nothing((length(ToSend)-1), none, SenderPid, Ref, ReferenceForSender) %OJO, none es un atomo y comparar integers contra atomos devuelve que el atomo es mayor, cuidado con esto
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

is_this_an_offer_i_can_refuse(State, Alpha, OldestKey, OldestValue, OldestTTL, OldetsTimestamp) ->
    FilteredState = filter_expired_values(State),
    IsExpired = is_expired(OldestTTL, OldetsTimestamp),
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
        {ok, {_, _, _}} -> %pertenece, a priori tengo el espacio
            put_and_spread(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids),
        error -> %no pertenece, veo de liberar espacio 
            FreeSpace = Alpha - maps:size(State),
            case FreeSpace of
                0 ->
                    filter_expired_or_deallocate_oldest(State, Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids)            
                _ -> 
                    put_and_spread(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids)
            end
    end

put_and_spread(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids) ->
    Timestamp = erlang:system_time(millisecond), %generamos el timestamp
    NewState = State#{Key => {Value, TTL, Timestamp}}, %guardamos la key en el nuevo estado
    SenderPid ! {ok, ReferenceForSender},
    Message = {spread_put, Key, Value, TTL, Timestamp}, %armamos el mensaje a enviar         
    send_to_replicas(ListaPids, Message, self()), %paso la lista de pids a enviar, la entry y me envio a mi
    NewState. %retorno el nuevo estado

filterExpiredValues(State) ->
    maps:filter(
        fun(_, {_, TTL, Timestamp}) ->
            not is_expired(TTL, Timestamp)
        end,
        State
    ).

filter_expired_or_deallocate_oldest(State, Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids) -> %ver argumentos
    FilteredState = filterExpiredValues(State),
    FreeSpace = Alpha - maps:size(FilteredState),
    case FreeSpace of 
        0 -> 
            deallocate_oldest_and_offer_to_other_replicas(State, Alpha, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids)
        _ -> 
            put_and_spread(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids)
    end.

deallocate_oldest_and_offer_to_other_replicas(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids) ->
    Oldest = maps:fold(
        fun(Key, {Value, TTL, Timestamp}, OldestSoFar) ->
            case OldestSoFar of
                none ->
                    {Key, Value, TTL, Timestamp};
                {_, _, _, OldTimestamp} when Timestamp < OldTimestamp ->
                    {Key, Value, TTL, Timestamp};
                _ ->
                    OldestSoFar
            end
        end,
        none,
        State
    )
    {OldestKey, OldestValue, OldestTTL, OldetsTimestamp} = Oldest
    spawn(fun() ->
        Message = {offer_key, OldestKey, OldestValue, OldestTTL, OldetsTimestamp},
        send_to_replicas(ListaPids, Message, self())
    end),
    StateWithFreeSpace = maps:remove(OldestKey, State),
    put_and_spread(StateWithFreeSpace, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids).

spread_put_handler(State, Alpha, Key, Value, TTL, Timestamp) ->
    case maps:find(Key, State) of
        {ok, {_, _, MyTimestamp}} -> %existe esa key en mis libros, reviso cual es mas nueva
            CurrentTime = erlang:system_time(millisecond),
            IsNewer = if %si ttl es infinity rompe la suma con ttl hay que separarlo
                TTL =:= infinity ->
                    MyTimestamp < Timestamp;
                true ->
                    (MyTimestamp < Timestamp) andalso ((Timestamp + TTL) > CurrentTime)
            end,
            if 
                IsNewer =:= true ->
                    State#{Key => {Value, TTL, Timestamp}};
                true ->
                    State
            end;
        error -> %no tengo esa key, veo que efectivamente no este expirado
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




