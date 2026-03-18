# Decisiones Arquitectónicas (NexusIQ)

Al diseñar esta prueba técnica orientada a un log inmutable para Digital Twins, se tomaron tres decisiones arquitectónicas cruciales para garantizar escalabilidad funcional, consistencia y robustez en un escenario de mantenimiento predictivo.

### 1. Sistema Basado Completamente en Event-Sourcing (Single-Table Strategy)

En lugar de crear múltiples tablas tradicionales (`machines`, `products`, `metrics`), el backend entero se diseña como un almacén inmutable de tipo *Append-Only* («Solo inserciones») en una única tabla PostgreSQL denominada `events`.

- **Por qué funciona para un Data Scientist:** La arquitectura permite la trazabilidad perfecta de los tiempos de los sensores y, más importante aún, ofrece reconstitución del estado exacto en un momento pasado de la máquina, posibilitando el **Time Travel** nativo sin tablas de versionado explícitas.
- **Por qué PostgreSQL con `JSONB`**: Se evita SQLite porque no da soporte nativo asincrónico para JSONB completo en el Driver de SQLAlchemy. Postgresql gestiona índices `JSONB` que permiten al *Correlation Engine* realizar consultas ultra-rápidas extrayendo variables dinámicas según se descubren, garantizando además bloqueos mínimos a las lecturas al insertar alta frecuencia (MVCC).

### 2. Idempotencia mediante Hashing Determinístico en el Pipeline

Para hacer robusto al sistema de ingesta (M1) a fallos en el sistema eléctrico o red en el entorno local u on-premise de las máquinas, se implementó una estrategia matemática para calcular las PK (`event_id` como UUID5) basados en fragmentos del CSV (El ID Único de producto + una firma criptográfica SHA-256 natural de todas las medidas).

- **El "Por Qué":** Si un batch falla a la mitad y se re-ejecuta el script, un simple `INSERT ON CONFLICT DO NOTHING` filtra los miles de eventos duplicados en base de datos.
- **Trade-off de recursos:** Esta técnica exige tiempo de CPU para hacer hashing, pero elimina la alternativa perniciosa de requerir la mantención de archivos stateful como "checkpoints" que suelen asincronizarse de la BD en caídas fatales.

### 3. Materialización Asincrónica del Digital Twin en la Base de Datos (Eventos Secundarios)

Para el Digital Twin (M2) decidimos no realizar el cálculo estadístico computacional iterando todos los miles de eventos origen de una máquina cada vez que se solicita por HTTP `GET /health`. Por el contrario, la aplicación inserta un segundo tipo de evento (`domain='TWIN_STATE'`) en la misma base de datos, en el cual guarda un *snapshot* o *vista materializada* de las propiedades promediadas (Pieces, Wear, Failure Rate, etc).

- **Beneficio Principal:** Consultar el estado actual de una máquina pasa a ser una transacción `O(1)` (`SELECT * FROM events WHERE ... LIMIT 1`) independiente de cuántos millones de logs originen la serie.
- **Beneficio para Data Science (M3):** Al separar el origen y los *insights*, permitimos a tu compañero trabajar tranquilamente consultando eventos `SOURCE` estáticos sin verse perturbado por escrituras paralelas de promedios de gemelos digitales.
