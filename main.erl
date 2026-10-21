-module(simple_kv).
-export([start_link/1, stop/1, put/3, put/4, delete/2, get/2]).
-spec start_link(pos_integer()) -> {ok, [pid()]}.
-spec stop([pid()]) -> ok.
-spec put(pid(), term(), term()) -> ok.
-spec put(pid(), term(), term(), non_neg_integer() | infinity) -> ok.
-spec delete(pid(), term()) -> ok.
-spec get(pid(), term()) -> {ok, term()} | not_found.

start_link(N) when N >= 0 -> 
    case N of
        0 -> return []
        _ -> pid = spawn(simple_kv, loop, #{})    
        return start_link(N-1) ++ [pid]
    end
    
loop(State) -> 
    receive 
        {put, Key, Value, SenderPid} -> 
            NewState = State#{Key => Value}
            SenderPid ! ok
            loop(NewState)
        {get, Key, SenderPid} ->
            case maps:find(Key, State) of
                {ok, Value} -> SenderPid ! {ok, Value}
                error -> 
                    
            loop(State)
        {delete, Key, SenderPid} -> 
            NewState = maps:remove(Key, State)
            SenderPid ! ok
            loop(NewState)
        stop -> 
            SenderPid ! ok
            loop(State)
    end
    



put(pid, key, Value) -> 
    pid ! {put, key, Value}

get(pid, key) -> 
    pid ! {get, key}

delete(pid, key) -> 
    pid ! {delete, key}

