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
    end.
    case Primero of 
        true -> todosConTodos(ListaPids, length(ListaPids))

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
        {put, Key, Value, SenderPid} -> 
            NewState = put_in_replicas(), 
            SenderPid ! ok,
            loop(NewState, ListaPids);
        {get, Key, SenderPid} ->
            case maps:find(Key, State) of
                {ok, Value} -> SenderPid ! {ok, Value};
                error -> {error, not_found}
            end,
            loop(State, ListaPids);
        {delete, Key, SenderPid} -> 
            NewState = maps:remove(Key, State),
            SenderPid ! ok,
            loop(NewState, ListaPids);
        {stop, SenderPid} -> 
            SenderPid ! ok,
    end.

put(Pid, Key, Value) -> 
    Pid ! {put, Key, Value, self()}.

get(Pid, Key) -> 
    Pid ! {get, Key, self()}.

delete(Pid, Key) -> 
    Pid ! {delete, Key, self()}.

put_in_replicas(State, Key, Value, TTL, ListaPids) ->
    CreatedAt = erlang:system_time(miliseconds)
    Entry = {Key => {Value, TTL, CreatedAt}} %FIX ME: habria que ver como mandarlo para que funcione con el pattern matching
    State#Entry %FIXME: vale esto?
    send_to_replicas(ListaPids, Entry, MyPid)
    
send_to_replcias(ToSend, Entry, MyPid) ->
    case ToSend of 
        [] -> ok;
        _ -> 
            [Head | Tail ] = ToSend,
            if 
                Head == MyPid -> 
                    ,todosConTodos(Tail, Entry, MyPid)
                true ->
                    Head ! Entry
                    todosConTodos(Tail, Entry, MyPid)
    end.
    