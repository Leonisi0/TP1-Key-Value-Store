-module(main_tests).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% FIXTURE: Setup y Cleanup
%% =============================================================================

setup_cluster() ->
    {ok, Pids} = main:start_link(3, 4),
    Pids.

cleanup_cluster(Pids) ->
    main:stop(Pids).

cluster_test_() ->
    {foreach,
     fun setup_cluster/0,
     fun cleanup_cluster/1,
     [

      fun test_01_get_inexistente/1,
      fun test_02_tipos_de_datos_erlang/1,
      fun test_03_sobrescritura_clave/1,
      fun test_04_borrado_clave_existente/1,
      fun test_05_borrado_idempotente/1,
      fun test_06_put3_equivale_a_put4_infinity/1,
      fun test_07_recreacion_clave_previamente_borrada/1,
      fun test_08_replicacion_asincronica_escritura/1,
      fun test_09_replicacion_asincronica_borrado/1,
      fun test_10_convergencia_escrituras_concurrentes/1,
      fun test_11_borrado_prevalece_sobre_escritura_demorada/1,
      fun test_12_expiracion_inmediata_ttl_cero/1,
      fun test_13_expiracion_ttl_finito/1,
      fun test_14_actualizacion_y_extension_de_ttl/1,
      fun test_15_desalojo_por_capacidad_y_busqueda_remota/1,
      fun test_16_preservar_75_porciento_mas_reciente/1,
      fun test_17_saturacion_total_descarte_sin_loop_infinito/1,
      fun test_18_orden_entrega_secuencial_fifo/1,
      fun test_19_correlacion_referencias_pedidos_concurrentes/1,
      fun test_20_ausencia_deadlock_atencion_concurrente/1
     ]}.


%% =============================================================================
%% 1. INTERFAZ Y OPERACIONES BÁSICAS (CRUD)
%% =============================================================================

test_01_get_inexistente([P1 | _]) ->
    {"Test 01: Lectura de una clave no almacenada devuelve not_found",
     fun() ->
         ?assertEqual(not_found, main:get(P1, clave_inexistente))
     end}.

test_02_tipos_de_datos_erlang([P1 | _]) ->
    {"Test 02: Permite almacenar y recuperar cualquier termino de Erlang como clave y valor",
     fun() ->
         ClaveCompleja = {usuario, 42, [admin]},
         ValorComplejo = #{datos => [1, 2, 3], activo => true},
         ?assertEqual(ok, main:put(P1, ClaveCompleja, ValorComplejo)),
         ?assertEqual({ok, ValorComplejo}, main:get(P1, ClaveCompleja))
     end}.

test_03_sobrescritura_clave([P1 | _]) ->
    {"Test 03: Escribir sobre una clave existente reemplaza su valor anterior",
     fun() ->
         ?assertEqual(ok, main:put(P1, config, valor_inicial)),
         ?assertEqual({ok, valor_inicial}, main:get(P1, config)),
         ?assertEqual(ok, main:put(P1, config, valor_nuevo)),
         ?assertEqual({ok, valor_nuevo}, main:get(P1, config))
     end}.

test_04_borrado_clave_existente([P1 | _]) ->
    {"Test 04: Borrar una clave existente hace que las lecturas posteriores devuelvan not_found",
     fun() ->
         ?assertEqual(ok, main:put(P1, clave_a_borrar, 1234)),
         ?assertEqual({ok, 1234}, main:get(P1, clave_a_borrar)),
         ?assertEqual(ok, main:delete(P1, clave_a_borrar)),
         ?assertEqual(not_found, main:get(P1, clave_a_borrar))
     end}.

test_05_borrado_idempotente([P1 | _]) ->
    {"Test 05: Borrar una clave que nunca existio retorna ok sin errores",
     fun() ->
         ?assertEqual(ok, main:delete(P1, clave_fantasma))
     end}.

test_06_put3_equivale_a_put4_infinity([P1 | _]) ->
    {"Test 06: put/3 almacena con TTL = infinity y no vence tras esperar",
     fun() ->
         ?assertEqual(ok, main:put(P1, clave_inf_1, valor_1)),
         ?assertEqual(ok, main:put(P1, clave_inf_2, valor_2, infinity)),
         timer:sleep(80),
         ?assertEqual({ok, valor_1}, main:get(P1, clave_inf_1)),
         ?assertEqual({ok, valor_2}, main:get(P1, clave_inf_2))
     end}.

test_07_recreacion_clave_previamente_borrada([P1, P2 | _]) ->
    {"Test 07: Una clave borrada puede volver a escribirse con un nuevo put posterior",
     fun() ->
         ?assertEqual(ok, main:put(P1, ciclo, valor_uno)),
         timer:sleep(20),
         ?assertEqual(ok, main:delete(P1, ciclo)),
         timer:sleep(20),
         ?assertEqual(not_found, main:get(P2, ciclo)),

         %% Recreación con un put nuevo (timestamp superior al tombstone)
         ?assertEqual(ok, main:put(P2, ciclo, valor_dos)),
         timer:sleep(30),
         ?assertEqual({ok, valor_dos}, main:get(P1, ciclo)),
         ?assertEqual({ok, valor_dos}, main:get(P2, ciclo))
     end}.


%% =============================================================================
%% 2. REPLICACIÓN Y CONVERGENCIA EVENTUAL
%% =============================================================================

test_08_replicacion_asincronica_escritura([P1, P2, P3]) ->
    {"Test 08: Un dato escrito en P1 se propaga asincronicamente y converge en P2 y P3",
     fun() ->
         ?assertEqual(ok, main:put(P1, color, azul)),
         timer:sleep(30),
         ?assertEqual({ok, azul}, main:get(P2, color)),
         ?assertEqual({ok, azul}, main:get(P3, color))
     end}.

test_09_replicacion_asincronica_borrado([P1, P2, P3]) ->
    {"Test 09: Un borrado en P2 se propaga y elimina el dato en P1 y P3",
     fun() ->
         ?assertEqual(ok, main:put(P1, color, azul)),
         timer:sleep(30),
         ?assertEqual(ok, main:delete(P2, color)),
         timer:sleep(30),
         ?assertEqual(not_found, main:get(P1, color)),
         ?assertEqual(not_found, main:get(P3, color))
     end}.

test_10_convergencia_escrituras_concurrentes([P1, P2, P3]) ->
    {"Test 10: Escrituras concurrentes en distintas replicas convergen deterministamente al mismo valor",
     fun() ->
         Self = self(),
         spawn(fun() -> Self ! {p1_done, main:put(P1, conflict_key, valor_p1)} end),
         spawn(fun() -> Self ! {p2_done, main:put(P2, conflict_key, valor_p2)} end),
         receive {p1_done, ok} -> ok end,
         receive {p2_done, ok} -> ok end,

         timer:sleep(50),
         {ok, Res1} = main:get(P1, conflict_key),
         {ok, Res2} = main:get(P2, conflict_key),
         {ok, Res3} = main:get(P3, conflict_key),
         ?assertEqual(Res1, Res2),
         ?assertEqual(Res2, Res3)
     end}.

test_11_borrado_prevalece_sobre_escritura_demorada([P1, P2 | _]) ->
    {"Test 11: Un borrado posterior prevalece sobre escrituras previas o demoradas (tombstone)",
     fun() ->
         ?assertEqual(ok, main:put(P1, item, original)),
         timer:sleep(20),
         ?assertEqual(ok, main:delete(P2, item)),
         timer:sleep(30),
         ?assertEqual(not_found, main:get(P1, item)),
         ?assertEqual(not_found, main:get(P2, item))
     end}.


%% =============================================================================
%% 3. TIME TO LIVE (TTL)
%% =============================================================================

test_12_expiracion_inmediata_ttl_cero([P1 | _]) ->
    {"Test 12: Un dato escrito con TTL = 0 vence de forma inmediata y devuelve not_found",
     fun() ->
         ?assertEqual(ok, main:put(P1, instantaneo, 999, 0)),
         ?assertEqual(not_found, main:get(P1, instantaneo))
     end}.

test_13_expiracion_ttl_finito([P1, P2 | _]) ->
    {"Test 13: Un dato con TTL finito es legible inicialmente y expira tras cumplirse el tiempo",
     fun() ->
         ?assertEqual(ok, main:put(P1, temp, valor_temp, 50)),
         ?assertEqual({ok, valor_temp}, main:get(P1, temp)),
         timer:sleep(70),
         ?assertEqual(not_found, main:get(P1, temp)),
         ?assertEqual(not_found, main:get(P2, temp))
     end}.

test_14_actualizacion_y_extension_de_ttl([P1, P2 | _]) ->
    {"Test 14: Sobrescribir una clave existente permite extender o renovar su vigencia de TTL",
     fun() ->
         %% Se almacena con 60 ms
         ?assertEqual(ok, main:put(P1, renovable, activo, 60)),
         timer:sleep(35),
         %% Antes de que expire (35 < 60), se renueva con 80 ms adicionales
         ?assertEqual(ok, main:put(P2, renovable, extendido, 80)),
         timer:sleep(35),
         %% Pasaron 70 ms totales desde la primera escritura; sin renovación ya habría vencido
         ?assertEqual({ok, extendido}, main:get(P1, renovable)),
         %% Tras agotar el nuevo TTL
         timer:sleep(60),
         ?assertEqual(not_found, main:get(P1, renovable))
     end}.


%% =============================================================================
%% 4. CAPACIDAD (ALFA), DESALOJO Y BÚSQUEDA REMOTA
%% =============================================================================

test_15_desalojo_por_capacidad_y_busqueda_remota([P1, P2 | _]) ->
    {"Test 15: Al superar Alfa (4) se desaloja localmente pero sigue accesible via consulta a otras replicas",
     fun() ->
         ?assertEqual(ok, main:put(P1, k1, v1)),
         timer:sleep(5),
         ?assertEqual(ok, main:put(P1, k2, v2)),
         timer:sleep(5),
         ?assertEqual(ok, main:put(P1, k3, v3)),
         timer:sleep(5),
         ?assertEqual(ok, main:put(P1, k4, v4)),
         timer:sleep(5),
         %% Quinto dato fuerza desalojo de k1 en P1
         ?assertEqual(ok, main:put(P1, k5, v5)),
         timer:sleep(30),

         ?assertEqual({ok, v1}, main:get(P1, k1)),
         ?assertEqual({ok, v5}, main:get(P2, k5))
     end}.

test_16_preservar_75_porciento_mas_reciente([P1 | _]) ->
    {"Test 16: Al desalojar, el 75% escrito mas recientemente sigue disponible",
     fun() ->
         %% Alfa = 4. El 75% corresponde a las 3 claves más recientes (c3, c4, c5).
         ?assertEqual(ok, main:put(P1, c1, v1)),
         timer:sleep(5),
         ?assertEqual(ok, main:put(P1, c2, v2)),
         timer:sleep(5),
         ?assertEqual(ok, main:put(P1, c3, v3)),
         timer:sleep(5),
         ?assertEqual(ok, main:put(P1, c4, v4)),
         timer:sleep(5),
         ?assertEqual(ok, main:put(P1, c5, v5)),
         timer:sleep(20),

         ?assertEqual({ok, v3}, main:get(P1, c3)),
         ?assertEqual({ok, v4}, main:get(P1, c4)),
         ?assertEqual({ok, v5}, main:get(P1, c5))
     end}.

test_17_saturacion_total_descarte_sin_loop_infinito([P1 | _]) ->
    {"Test 17: La saturacion global de almacenamiento descarta datos viejos sin generar loops de mensajes",
     fun() ->
         %% 3 nodos * Alfa 4 = capacidad teórica global de 12. Escribimos 18 claves.
         lists:foreach(fun(I) ->
             main:put(P1, {sat_key, I}, {sat_val, I}),
             timer:sleep(2)
         end, lists:seq(1, 18)),

         timer:sleep(50),
         %% El cluster debe mantenerse responsivo y atender operaciones normalmente
         ?assertEqual(ok, main:put(P1, post_saturacion, test_ok)),
         ?assertEqual({ok, test_ok}, main:get(P1, post_saturacion))
     end}.


%% =============================================================================
%% 5. CONCURRENCIA, MENSAJERÍA Y AUSENCIA DE DEADLOCK
%% =============================================================================

test_18_orden_entrega_secuencial_fifo([P1 | _]) ->
    {"Test 18: Dos escrituras consecutivas desde el mismo proceso mantienen el orden FIFO",
     fun() ->
         ?assertEqual(ok, main:put(P1, version, v1)),
         ?assertEqual(ok, main:put(P1, version, v2)),
         ?assertEqual({ok, v2}, main:get(P1, version))
     end}.

test_19_correlacion_referencias_pedidos_concurrentes([P1 | _]) ->
    {"Test 19: Clientes concurrentes no cruzan sus respuestas gracias a referencias unicas (make_ref)",
     fun() ->
         Self = self(),
         main:put(P1, k_par, par),
         main:put(P1, k_impar, impar),

         lists:foreach(fun(I) ->
             spawn(fun() ->
                 Key = case I rem 2 of 0 -> k_par; 1 -> k_impar end,
                 Expected = case I rem 2 of 0 -> par; 1 -> impar end,
                 {ok, Val} = main:get(P1, Key),
                 Self ! {cliente, I, Val == Expected}
             end)
         end, lists:seq(1, 20)),

         lists:foreach(fun(I) ->
             receive
                 {cliente, I, true} -> ok
             after 500 ->
                 ?assert(false)
             end
         end, lists:seq(1, 20))
     end}.

test_20_ausencia_deadlock_atencion_concurrente([P1 | _]) ->
    {"Test 20: Una replica puede atender nuevos pedidos concurrentes mientras resuelve consultas de otras",
     fun() ->
         Self = self(),
         spawn(fun() -> 
             Res = main:get(P1, clave_desconocida_global),
             Self ! {cliente_a, Res}
         end),
         
         ?assertEqual(ok, main:put(P1, clave_inmediata, rapido)),
         ?assertEqual({ok, rapido}, main:get(P1, clave_inmediata)),

         receive
             {cliente_a, ResA} -> ?assertEqual(not_found, ResA)
         after 1000 ->
             ?assert(false)
         end
     end}.


%% =============================================================================
%% 6. ESCENARIOS EXTENDIDOS Y VALIDACIONES DE API (SIN FIXTURE)
%% =============================================================================

exceptions_test_() ->
    [
     {"Test 21: start_link/2 falla con function_clause si N es menor o igual a cero",
      fun() ->
          ?assertError(function_clause, main:start_link(0, 5)),
          ?assertError(function_clause, main:start_link(-1, 5))
      end},
     {"Test 22: start_link/2 falla con function_clause si Alfa es invalido",
      fun() ->
          ?assertError(function_clause, main:start_link(3, 0)),
          ?assertError(function_clause, main:start_link(3, -5)),
          ?assertError(function_clause, main:start_link(3, "invalido"))
      end},
     {"Test 23: put/4 falla con function_clause si el TTL es un entero negativo",
      fun() ->
          {ok, Pids} = main:start_link(1, 5),
          [P] = Pids,
          ?assertError(function_clause, main:put(P, clave, valor, -10)),
          main:stop(Pids)
      end},
     {"Test 24: put/4 falla con function_clause si el TTL no es entero ni infinity",
      fun() ->
          {ok, Pids} = main:start_link(1, 5),
          [P] = Pids,
          ?assertError(function_clause, main:put(P, clave, valor, "invalido")),
          main:stop(Pids)
      end},
     {"Test 25: stop/1 detiene ordenadamente todos los procesos del cluster",
      fun() ->
          {ok, Pids} = main:start_link(2, 5),
          ?assertEqual(ok, main:stop(Pids)),
          timer:sleep(20),
          lists:foreach(fun(Pid) -> ?assertEqual(false, is_process_alive(Pid)) end, Pids)
      end},
     {"Test 26: cluster grande (N = 6) replica y converge en todos los nodos",
      fun() ->
          {ok, Pids} = main:start_link(6, 4),
          [P1 | Resto] = Pids,
          ?assertEqual(ok, main:put(P1, broadcast_key, broadcast_val)),
          timer:sleep(50),
          lists:foreach(fun(P) ->
              ?assertEqual({ok, broadcast_val}, main:get(P, broadcast_key))
          end, Resto),
          main:stop(Pids)
      end}
    ].