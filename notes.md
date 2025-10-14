- Resolver errores
- Añadir en process question dentro del background task, un mensaje inicial al usuario de thinking.
- Añadir WK0 y WK-1 en el dataframe para calculo de variaciones semanales
- Duda sobre el WoW, cuando queremos saber WoW, nos referimos a semana wk0 vs wk-1 o a semana x del año vs semana anterior, porque no va a ser lo mismo, si algo no cambia de una semana a otra todavia (dias antes del miercoles), la vas a preguntar al bot por WoW cuando todavia no hay el WoW al que se refiere el usuario.
- Afinamiento del thread history, que dé mas peso a lo reciente


Sobre Slack:
- Los events los deberia guardar en una base de datos de firestore/reddis para mantener persistencia entre encendidos
- Los mensajes editados/borrados reaccionar a ellos.

De los threads:
- Yo necesito el async original para manejar distintas requests a la vez, y luego el background para poder responder primero y luego ejecutar, esto es lo que diferencia a background de async, y luego seria bueno meter hilos para tareas simultaneas de I/O, como runear la query y llamar al primer llm, o si hay dos llms simultaneos.


Para bajar coste:
- Definir en el backend que operaciones puede hacer tu modelo y hacer los calculos sobre la query que tiras uno mismo en el backend, centrarse solo en select metrics y where clauses, y que te diga la operacion a realizar o incluso varias operaciones a realizar, despues pasarselo de nuevo al modelo para que de una respuesta en lenguaje natural.