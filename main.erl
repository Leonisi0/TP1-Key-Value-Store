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
            Lista = start_superLink(N-1, Primero) ++ [Pid]
    end.
    case Primero of 
        true -> todosConTodos(Lista, length(Lista))

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
    
loop(State, Lista) -> 
    receive 
        {put, Key, Value, SenderPid} -> 
            NewState = State#{Key => Value},
            SenderPid ! ok,
            loop(NewState, Lista);
        {get, Key, SenderPid} ->
            case maps:find(Key, State) of
                {ok, Value} -> SenderPid ! {ok, Value};
                error -> {error, not_found}
            end,
            loop(State, Lista);
        {delete, Key, SenderPid} -> 
            NewState = maps:remove(Key, State),
            SenderPid ! ok,
            loop(NewState, Lista);
        {stop, SenderPid} -> 
            SenderPid ! ok,
    end.

put(Pid, Key, Value) -> 
    Pid ! {put, Key, Value, self()}.

get(Pid, Key) -> 
    Pid ! {get, Key, self()}.

delete(Pid, Key) -> 
    Pid ! {delete, Key, self()}.

