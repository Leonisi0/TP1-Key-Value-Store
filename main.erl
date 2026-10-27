-module(simple_kv).
-export([start_link/1, stop/1, put/3, put/4, delete/2, get/2]).
-spec start_link(pos_integer()) -> {ok, [pid()]}.
-spec stop([pid()]) -> ok.
-spec put(pid(), term(), term()) -> ok.
-spec put(pid(), term(), term(), non_neg_integer() | infinity) -> ok.
-spec delete(pid(), term()) -> ok.
-spec get(pid(), term()) -> {ok, term()} | not_found.

start_link(N) when N >= 0 -> 
    star_superLink(N,true)

star_superLink(N,Primero) when N >= 0 -> 
    case N of
        0 -> [];
        _ ->
            Pid = spawn(fun init/0),    
            ListaPids = start_superLink(N-1, Primero) ++ [Pid]
    end
    case Primero of 
        true -> todosConTodos(ListaPids, length(ListaPids)).

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
    
loop(State, ListaPids) -> 
    receive 
        {put, Key, Value, TTL, SenderPid, ReferenceForSender} -> %FIXME: estamos devolviendo ok al que solicito?
            NewState = put_handler(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids), 
            loop(NewState, ListaPids);
        {spread_put, Key, Value, TTL, Timestamp} -> %FIXME: estamos devolviendo ok al que solicito?
            NewState = spread_put_handler(State, Key, Value, TTL, Timestamp),
            loop(NewState, ListaPids);

        {get, Key, SenderPid, ReferenceForSender} -> %FIXME IMPORTANTE: el get deberia devolver si tiene una local unexpired o ver cual es la mas nueva??? 
            get_handler(State, Key, SenderPid, ReferenceForSender)
            loop(State, ListaPids);
        {spread_get, Key, SenderPid, ReferenceForSender} ->
            spread_get_handler() %FIXME: PONER LOS ARGUMENTOS
            loop(State, ListaPids)
         
        {delete, Key, SenderPid} -> 
            NewState = maps:remove(Key, State),
            SenderPid ! ok,
            loop(NewState, ListaPids);
        {stop, SenderPid} -> 
            SenderPid ! ok
    end.

put(Pid, Key, Value) -> 
    Ref = make_ref(), %para identificar cuando me responda quien me respondio ok
    Pid ! {put, Key, Value, self(), Ref},
    receive 
        {ok, Ref} -> ok %ok
    end.

get(Pid, Key) -> 
    Pid ! {get, Key, self()}.

delete(Pid, Key) -> 
    Pid ! {delete, Key, self()}.


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
    
%%%%%%%%%% HANDLERS %%%%%%%%%%

get_handler(State, ListaPids, Key, SenderPid, ReferenceForSender) ->
    case maps:find(Key, State) of
        {ok, {Value, TTL, Timestamp}} ->
            CurrentTime = erlang:system_time(millisecond),
            IsExpired = (TTL =/= infinity) andalso ((Timestamp + TTL) =< CurrentTime),
            if 
                IsExpired ->
                    fork_and_seek();
                true ->
                    SenderPid ! {ReferenceForSender, ok, Value} 
            end;
        error ->
            fork_and_seek()
    end.

fork_and_seek(ToSend, Key, SenderPid) -> %hay que ver que argumentos me paso
    Ref = make_ref(),
    MyPid = self(), %guardamos nuestro pid antes del fork para que no mire su pid sino el del padre (nosotros)
    spawn(fun() ->
        Message = {spread_get, Key, self(), Ref}, %aca adentro le pasamos nuestro pid osea el del fork
        send_to_replicas(ToSend, Message, MyPid), %con esta me pongo a preguntarle a todos, no se si puedo usar esta
        xxx((length(ToSend)-1), none, SenderPid) %OJO, none es un atomo y comparar integers contra atomos devuelve que el atomo es mayor, cuidado con esto
    end).

xxx(Iterator, NewestValue, SenderPid) ->
    case Iterator of
        0 ->
            if NewestValue =:= none -> 
        _ ->
    end.

    receive
        {ReferenceForSender, Value, TTL, Timestamp} ->
            NewestValue = newest_unexpired_between(NewestValue, Value, TTL, Timestamp)
        {ReferenceForSender, not_found} ->
        
        xxx(Iterator-1, NewestValue, SenderPid)
    end.



put_handler(State, Key, Value, TTL, SenderPid, ReferenceForSender, ListaPids) ->
    Timestamp = erlang:system_time(millisecond), %generamos el timestamp
    NewState = State#{Key => {Value, TTL, Timestamp}}, %guardamos la key en el nuevo estado
    SenderPid ! {ok, ReferenceForSender},
    Message = {spread_put, Key, Value, TTL, Timestamp}, %armamos el mensaje a enviar         
    send_to_replicas(ListaPids, Message, self()), %paso la lista de pids a enviar, la entry y me envio a mi
    NewState. %retorno el nuevo estado

spread_put_handler(State, Key, Value, TTL, Timestamp) ->
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

