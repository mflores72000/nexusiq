# Decisiones de Arquitectura (NexusIQ)

Para construir un sistema lo suficientemente robusto para un entorno industrial (fábricas, sensores, mantenimiento predictivo), tuvimos que tomar tres decisiones críticas que sacrifican algunas comodidades típicas a cambio de ganar extrema estabilidad.

### 1. Usar una Única Tabla Maestra de "Solo Lectura/Escritura" (Event Sourcing)

En lugar de construir el clásico esquema con múltiples tablas (una tabla de `maquinas`, otra de `productos`, otra de `medidas`), decidimos meter absolutamente todo en una sola tabla de PostgreSQL llamada `events`. Y la regla de oro es: **Nunca se usa el comando UPDATE**. Solo se agregan filas nuevas.

* **El beneficio:** Si un Data Scientist quiere saber cómo estaba la máquina "L" ayer al mediodía, no tiene que adivinar ni pelear con tablas sobrescritas. Tienes una máquina del tiempo perfecta ("Time-Travel") porque la base de datos funciona como un diario de vida cronológico.
* **El costo (Trade-off):** La base de datos crece hacia abajo muy rápido, y extraer el estado "actual" nos obliga a leer la historia en lugar de simplemente mirar una celda actualizada.

### 2. Generar IDs infalibles para evitar datos duplicados (Idempotencia)

Las fábricas suelen tener caídas de red. Si el script que lee el archivo CSV se corta a la mitad y lo volvemos a encender, el mayor riesgo era que empezara a guardar las mediciones de los sensores dos veces en la base de datos (corrompiendo el modelo de ML de tu compañero).

* **El beneficio:** En vez de dejar que la base de datos asigne IDs clásicos (`1, 2, 3...`), programamos el backend para que le fabrique una huella digital única (UUID) a cada fila del CSV combinando la temperatura, el modelo y su código. Así, si el script intenta guardar accidentalmente el mismo dato dos veces, PostgresSQL reconoce la huella repetida y la rebota silenciosamente sin romper nada.
* **El costo (Trade-off):** Generar estas huellas toma un poco más de energía de cálculo (CPU) antes de guardar el dato, haciendo la ingesta ligeramente más lenta.

### 3. Tomar "Fotografías" del estado de las máquinas (Snapshots del Twin)

En vez de obligar al servidor web a hacer un conteo matemático gigante sumando miles de eventos antiguos de temperatura cada vez que alguien entra a ver la pantalla del navegador (`/health/view`), decidimos que el backend guarde "resúmenes" de forma automática.

* **El beneficio:** Cuando alguien abre el Dashboard en su computadora, la respuesta del servidor es instantánea, sin importar si la máquina lleva procesadas 5 piezas o 5 millones de piezas. El servidor solo lee la "última foto" guardada de los promedios.
* **El costo (Trade-off):** Usamos más espacio en el disco duro, ya que por cada tanda nueva de datos de los sensores que entra, tenemos que crear un bloque nuevo en la base de datos exclusivamente para guardar el resumen fotográfico.
