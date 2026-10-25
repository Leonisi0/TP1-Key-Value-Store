(Hagan el informe acá)
(Previo a informe cosas que veo de leer el tp):
-El TTL te obliga a tener que guardar alguna especie de timestamp de cuando lo escribis porque tenes que constrastar contra algo si ya paso el tiempo
-Esto del timestamp creo que tambien te sirve para resolver cuando tenes dos mensajes que cambian la misma variable para decidir, para el borrado CREO que tambien.
-Unificar lenguaje

Enfoque top down pensando como deberia funcionar un put:
sin pensar aun en concurrencia, habria que simplemente hacer pattern matching donde si nos dicen put lo guardamos en el diccionario y loopeamos sobre nuestro estado, pero en este caso al haber muchas, tenemos que entonces resolverlo guardando un timestamp/createdAt para saber cual ocurrio primero, y si te llega un put pero ocurrio antes que otro que ya lo pisa lo ignoras porque ya quedo viejo, por eso necesitas alguna especie de tiempo para comparar, y ademas existe el
TTL que hace que tengas que dar por muerto un dato si ya expiro, todavia estoy ignorando como seria la implementacion con la cache o la regla de maximo n elems.
el borrado funcionaria de manera similar intuyo, el get hay que propagarlo y quedarse con el mas nuevo aun no pense esto demasiado. podriamos paralelizar estos...