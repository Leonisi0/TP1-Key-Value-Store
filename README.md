(Hagan el informe acá)
Dejamos aca un link al overleaf con el archivo: https://www.overleaf.com/project/6af29f2ba25c41c2717fb19a/share#c325920cf7add7c32aaffdfacb9e1fe37ae00bf2e74da4b8

En el catastrofico caso donde no puedan acceder al mismo por si las dudas dejamos el codigo fuente del informe en LaTex:

\documentclass[11pt,a4paper]{article}

\usepackage[utf8]{inputenc}
\usepackage[spanish,es-tabla]{babel}
\usepackage[margin=2.5cm]{geometry}
\usepackage{amsmath,amssymb}
\usepackage{booktabs}
\usepackage{tabularx}
\usepackage{hyperref}
\usepackage{microtype}

\newcolumntype{Y}{>{\raggedright\arraybackslash}X}

\title{\textbf{Informe de Trabajo Práctico de Base de Datos Key-Value Replicada}\\
\large Programación Concurrente y Paralela}
\author{Martin Leon Cuadrado Bertollo \& Valentin Aguilar Galeano}

\begin{document}

\maketitle

\section{Estructura de las réplicas}

Para las replicas decidimos que en el diccionario cada Key este asociada a una tupla donde efectivamente se guarde el valor ingresado via API junto a su TTL, y el timestamp, el cual es necesario para poder garantizar la concurrencia, resolver conflictos y tiempos de vida de los valores. La tupla que usamos asociada a la Key es {Value, TTL, Timestamp}. El timestamp se guarda en el momento previo a guardar efectivamente la tupla dentro del diccionario local (cuando ya se verifico que se cuenta con el espacio suficiente y se puede guardar) y es con el que va a propagarse hacia el resto de replicas para resolver posibles conflictos. Cada replica se inicializa con una tupla que representa su estado, este esta compuesto por:
\[
\text{Estado} = \{\texttt{State}, \texttt{ListaPids}, \texttt{Alpha}\}
\]

\begin{itemize}
    \item \textbf{\texttt{State}:} El State es el diccionario que contiene las key-value
    \item \textbf{\texttt{ListaPids}:} La ListaPids contiene los PIDs de todos las replicas del sistema, esta lista es estatica, es decir, no cambia en ningun momento. Por lo que cuando se iniciliza la API se hace con una cantidad prefijada de replicas las cuales se asume que no fallan ni finalizan hasta que la API se apague, esta lista es la "guia telefonica" que permite a la replica comunicarse via mensajes con el resto y propagar las distintas funciones
    \item \textbf{\texttt{Alpha}:}  Por ultimo tenemos el Alpha que es el numero entero que determina el tamaño del diccionario y nos va a indicar cuando nos hallamos quedado
sin tamaño a la hora de querer hacer un put o que nos ofrezcan guardar un valor desalojado de otra replica.
\end{itemize}

Por ultimo para representar los borrados estos se guardan con la misma key y con valor
$deleted$ y TTL infinito dentro de la tupla, es decir, es no-destructivo. La justificacion detras de esto se presenta en una de las secciones subsiguientes donde vemos la resoluciones de conflictos
y borrados.
    

\section{Protocolo de comunicación}

El protocolo de comunicacion dentro del main loop es el siguiente, tenemos mensajes expuestos, que son aquellos que llegan via API, e internos que van a ser los encargados de propagar/recibir
los cambios que se hagan sobre los diccionarios locales de cada replica. entre los expuestos tenemos $put, get$ y $delete$, estos van a ser los enviados por el cliente donde ademas de contener la 
key/value en su firma van a tener el PID de quien envia el mensaje para que el receptor del mensaje (la replica) pueda responder si se pudo realizar la accion con exito, y el numero de referencia, 
para que el emisor pueda distinguir de quien es el mensaje que recibio, porque si alguien le llega a enviar un mensaje con la misma firma tiene que ser capaz de distinguir si es la Response del 
pedido particular que hizo a la API. Luego en los mensajes "privados" de las replicas tenemos un spread particular para cada accion ($put, get, delete$) que es el encargado replicar los cambios en 
el sistema de manera global, los cuales vamos a ver mas en profundidad en seccion de estrategia de resolucion de conflictos y borrados. Por ultimo tenemos otro mensaje interno que es el de $offer$
que es el encargado de ofrecer la tupla desalojada de alguna replica hacia las otras. 

\begin{table}[h!]
\centering
\small
\begin{tabularx}{\textwidth}{p{2.3cm} p{2cm} p{2.2cm} p{3.2cm} Y}
\toprule
\textbf{Mensaje} & \textbf{Emisor} & \textbf{Receptor} & \textbf{Contenido} & \textbf{Efecto en receptor} \\
\midrule
\texttt{configure} & Proceso padre & Replica local & \texttt{\{configure, Pids\}} & Inicializa la replica local y comienza el ciclo de $loop$. \\
\addlinespace
\texttt{put} & Cliente API & Replica local & \texttt{\{put, K, V, TTL, Pid, Ref\}} & Verifica espacio ($\alpha$), difunde a los pares y responde \texttt{\{ok, Ref\}}. \\
\addlinespace
\texttt{spread\_put} & Replica local & Replicas pares & \texttt{\{spread\_put, K, V, TTL, Ts\}} & Compara timestamps para saber con cual quedarse localmente (se queda con el mayor timestamp/mas reciente). \\
\addlinespace
\texttt{get} & Cliente API & Replica local & \texttt{\{get, K, Pid, Ref\}} & Si el dato no expiro ni esta borrado responde directo, sino el proceso se forkea y busca la version mas nueva que exista entre todas las replicas. \\
\addlinespace
\texttt{spread\_get} & Spawn de Replica local & Replicas pares & \texttt{\{spread\_get, K, WPid, Ref\}} & Busca el valor en su diccionario local y responde con la tupla asociada o $not found$. \\
\addlinespace
\texttt{delete} & Cliente API & Replica local & \texttt{\{delete, K, Pid, Ref\}} & Guarda la tupla asociada a la key con deleted y TTL infinito, difunde a pares y confirma \texttt{\{ok, Ref\}}. \\
\addlinespace
\texttt{spread\_delete} & Replica local & Replicas pares & \texttt{\{spread\_delete, K, Ts\}} & Compara timestamps y se
queda con el $delete$ si es mas reciente (mayor timestamp) que el del valor que tiene localmente. \\
\addlinespace
\texttt{offer\_key} & Replica local & Replicas pares & \texttt{\{offer\_key, K, V, TTL, Ts\}} & La receptora hace limpieza de vencidos, si tiene cupo ($<\alpha$) incorpora la clave, si esta llena no hace nada. \\
\addlinespace
\texttt{stop} & Cliente API & Todas las replicas & \texttt{\{stop, SenderPid\}} & Responde confirmacion y finaliza la recursión de $loop$. \\
\bottomrule
\end{tabularx}
\end{table}

\section{Resolucion de conflictos y consistencia eventual}
\subsection{Resolución de conflictos y borrados}

Para resolver el problema de escrituras concurrentes recibidas en distinto orden por réplicas distintas, se aplica un criterio determinista que elige siempre la misma versión:
\begin{itemize}
    \item \textbf{Marcas temporales:} Cada escritura local en \texttt{put\_and\_spread/7} y \texttt{delete\_handler/5} asigna un \textit{timestamp} físico.
    \item \textbf{Desempate determinista:} Ante dos versiones concurrentes, se prioriza aquella con mayor marca temporal (\texttt{Timestamp > MyTimestamp}). Si ambas coinciden, se desempata evaluando el orden de términos sobre los valores (\texttt{Value > MyValue}).
\end{itemize}

Para la gestión de borrados concurrentes y evitar que un \texttt{put} demorado sobrescriba una eliminación previa, el sistema emplea marcas de borrado (\textit{tombstones}):
\begin{itemize}
    \item En \texttt{delete\_handler/5} y \texttt{spread\_delete\_handler/3} se almacena la tupla \texttt{\{deleted, infinity, Timestamp\}} al eliminar una clave.
    \item En \texttt{spread\_delete\_handler/3}, si \texttt{DeleteTimestamp >= LocalTimestamp}, la operación de borrado prevalece, impidiendo la resurrección de claves eliminadas.
\end{itemize}

Si bien este mecanismo resuelve las condiciones de carrera en los borrados, estos registros ocupan espacio real dentro del diccionario que podria ser usado por una key valida (no borrada). Por lo que podria llegar a ocurrir en caso de se hagan muchos borrados que los diccionarios de las replicas se llenen de valores inutiles, esto es basicamente un trade off para resolver condiciones de carrera en borrados. En caso de llenarse de borrados igualmente el mecanismo de desalojo podria comenzar a ofrecer valores borrados y en caso de que efectivamente no haya espacio en el sistema, que nadie los tome y se tiren. Una posible solucion podria ser un garbage collection que corra periodicamente y limpie este tipo de valores si existe alguna manera de confirmar que efectivamente ya se propagaron.

\subsection{Consistencia eventual}

El sistema garantiza que, una vez que cesan las escrituras y finaliza el intercambio de mensajes, todas las réplicas convergen al mismo estado:
\begin{itemize}
    \item \textbf{Propagación asincrónica:} Todo cambio local dispara asincrónicamente \texttt{send\_to\_replicas/3} enviando mensajes \texttt{\{spread\_put, ...\}} o \texttt{\{spread\_delete, ...\}} hacia el resto de las réplicas.
    \item \textbf{Efecto de la capacidad $\alpha$:} Debido al límite $\alpha$, una réplica puede desalojar claves (\texttt{maps:remove/2}) ante falta de espacio; en consecuencia, los mapas locales no siempre serán idénticos byte a byte si la cantidad de claves activas supera dicho tope.
    \item \textbf{Consistencia en lecturas:} Para garantizar consistencia hacia el usuario, \texttt{get/2} recurre a \texttt{fork\_and\_seek/4} cuando una clave no se encuentra en el nodo local, consultando concurrentemente a las demás réplicas para recolectar y devolver el valor canónico más reciente.
\end{itemize}
\section{Mecanismo de deteccion de vencimiento (TTL)}
La deteccion de vencimiento es $lazy$ dado que no se esta revisando activamente cuando hay valores vencidos, sino que esta limpieza se pospone hasta que sea estrictamente necesario, lo cual es cuando ya no queda espacio dentro del diccionario. El mecanismo de deteccion y limpieza de vencimiento se dispara en dos metodos en particular: cuando se realiza un put y no se tiene espacio, se aprovecha para filtrar los vencidos y reintentar para ver si se libero espacio; Y cuando se le ofrece un valor desalojado y no se tiene espacio, ahi tambien se filtran los vencidos en caso de contar sin espacio. El mecanismo que se utiliza para evaluar el vencimiento es mediante el $timestamp$, que es el unico valor de metadata que se guarda junto al $value$ y el $TTL$ en la tupla del diccionario. El $timestamp$ se toma en el momento en el que se guarda localmente el valor en el diccionario de la replica particular a la que se le envio el mensaje de put. Luego en el filtrado se iteran todos los valores del diccionario y se evalua para cada uno que no tenga $TTL$ infinito y que la suma del $Timestamp$ y el $TTL$ sea menor o igual al $currentTime$, el cual se toma el reloj del sistema. Este chequeo de vencimiento a su vez tambien se realiza en el get, donde si el dato esta vencido tanto en el diccionario local como en el resto de replicas del sistema se responde como $not found$; Y en la replicacion de un put, donde se revisa que efectivamente el valor que me llego no se haya vencido (como no hay orden en el cual leo los mensajes que me llegaron, si tarde mucho en leer un mensaje de replicacion de un put puede ser que ya este vencido para cuando lo quiera guardar y en ese caso directamente no se guarda).
\section{Capacidad y mecanismo de desalojo}
La capacidad se define mediante $\alpha$ el cual determina artificialmente cual es la capacidad para cada diccionario local de las replicas, los momentos donde efectivamente se utiliza el mismo son cuando se realizan inserts dentro del diccionario para saber si efectivamente son legales o si se debe hacer una limpieza antes, el mecanismo de desalojo se activa solamente cuando llega un put via API, el mismo funciona tomando el $Timestamp$ mas antiguo del que tiene registro, lo elimina de su diccionario local y lo ofrece al resto de replicas. Para evitar loops infinitos decidimos que cuando a una replica le llega el aviso de una nueva escritura y no tiene espacio (luego de limpiar vencidos), entonces no desaloja a nadie y rechaza el dato silenciosamente, esto tambien ocurre con los ofrecimientos de keys desalojadas, donde si no tenes espacio luego del purgado de vencidos simplemente se omite el ofrecimiento. Por lo que un dato desalojado podria dejar de existir en el caso extremo de que se desaloje y todo el resto de replicas esten llenas con datos sin expirar. Esta decision hace que las replicas no queden en un bucle donde todas se estan pasando datos desalojados en cascada y rompan el sistema. Por ultimo se decidio cumplir con el umbral de mantener de minima el 75% mas nuevo desalojando solamente el mas viejo, facilitando asi el mecanismo de ofrecimiento y deasalojo.
\section{Algoritmo para get}
\begin{enumerate}
    \item \texttt{get/2}: Recibe el \texttt{Pid} de la réplica y la \texttt{Key} solicitada. Genera una referencia única mediante \texttt{make\_ref()} y envía el mensaje \texttt{\{get, Key, self(), Ref\}}, bloqueándose a la espera de la respuesta con dicha referencia.
    
    \item \texttt{loop/3}: Al recibir el mensaje \texttt{get}, la réplica delega la atención en \texttt{get\_handler/5} y continúa inmediatamente en el ciclo \texttt{loop/3} para seguir procesando otros mensajes concurrentes.
    
    \item \texttt{get\_handler/5}: Consulta la clave en el mapa local:
    \begin{itemize}
        \item Si la clave figura como \texttt{deleted}, responde \texttt{\{Ref, not\_found\}} de forma directa.
        \item Si el valor existe y no está expirado, responde inmediatamente con \texttt{\{Ref, \{ok, Value\}\}}.
        \item Si no existe en el nodo (\texttt{error}) o se encuentra expirado, delega la búsqueda en las demás réplicas mediante \texttt{fork\_and\_seek/4}.
    \end{itemize}

    \item \texttt{fork\_and\_seek/4}: Realiza un \texttt{spawn} de un proceso auxiliar efímero para evitar el bloqueo del bucle principal del nodo. Este proceso genera una referencia interna y transmite concurrentemente el mensaje \texttt{\{spread\_get, Key, self(), Ref\}} a todas las demás réplicas.

    \item \texttt{spread\_get\_handler/4}:Cada réplica consulta su estado interno:
    \begin{itemize}
        \item Si almacena una marca de borrado, responde con la tupla \texttt{\{deleted, TTL, Timestamp\}}.
        \item Si el valor existe y no ha expirado, responde con \texttt{\{Value, TTL, Timestamp\}}.
        \item Si la clave no se encuentra o ya expiró, retorna \texttt{not\_found}.
    \end{itemize}

    \item \texttt{newest\_unexpired\_between/4}: A medida que el proceso auxiliar recolecta las respuestas de cada nodo:
    \begin{itemize}
        \item Descarta cualquier valor que haya expirado al momento de ser recibido.
        \item Compara la marca temporal (\textit{timestamp}) del valor entrante contra el más reciente acumulado hasta el momento, actualizando el candidato ganador.
        \item Una vez recibidas las respuestas de todas las réplicas, envía el resultado definitivo (\texttt{\{ok, Value\}} o \texttt{not\_found}) al proceso cliente original (\texttt{SenderPid}).
    \end{itemize}
\end{enumerate}

Para garantizar la ausencia de deadlocks, delegamos la recoleccion de respuestas a un fork del proceso (replica) y la misma retorna inmediatamente a loop. Esto asegura que todos los nodos permanezcan reactivos para procesar nuevos pedidos via API o responder mensajes de sus pares, eliminando la posibilidad de bloqueos por espera circular entre procesos.

\end{document}