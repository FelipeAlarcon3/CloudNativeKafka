# Kafka en producción para CloudNativeOrders (DSY1107 · 3.3)

Casos prácticos del repaso **3.3.1 y 3.3.2**: escalar, replicar, retener y afinar.
Los eventos tienen la misma forma que el `PedidoCreado` que hoy publica `src/handlers/createOrder.js` en SNS
(`type`, `eventId`, `timestamp`, `pedidoId` + el pedido completo), así que sirven de base para migrar esa publicación a Kafka.

Pregunta guía: ¿qué le pasa a CloudNativeOrders cuando crece, cuando se cae un servidor y cuando se llena el disco?

## Estructura

```
kafka/
├─ docker-compose.yml          <- 1 broker + ZooKeeper + Kafka UI (casos 1, 3 y 4)
├─ docker-compose.cluster.yml  <- 3 brokers + ZooKeeper + Kafka UI (caso 2)
├─ zk.sh                       <- quién es el controlador y qué brokers viven (lee ZooKeeper)
├─ shared/kafka.js             <- conexión, topics y generador de pedidos de ejemplo
└─ scripts/
    ├─ setup.js        npm run setup         crea orders.events (3 part.) y orders.events.dlt
    ├─ generar.js      npm run generar       productor de pedidos (acks=all)
    ├─ report.js       npm run report        consumidor del grupo "report" (+ DLT)
    ├─ lag.js          npm run lag           monitor de consumer lag
    ├─ particiones.js  npm run particiones   sube las particiones a 6
    ├─ replicas.js     npm run replicas      líder / réplicas / ISR por partición
    ├─ retencion.js    npm run retencion     demo de retención (low sube)
    └─ carga.js        npm run carga         prueba de carga del productor
```

Requisitos: Docker Desktop y Node 18+. Kafka UI: <http://localhost:8081>

```bash
cd kafka
npm install
docker compose up -d      # espera ~20 s
npm run setup
```

## Proyecto real: los pedidos de la API publican en Kafka (prueba local)

`src/handlers/createOrder.js`, `updateOrder.js` y `deleteOrder.js` ahora llaman a `src/utils/kafkaPublisher.js`, que publica
`PedidoCreado`, `PedidoActualizado` y `PedidoEliminado` en `orders.events` (clave = id del pedido, `acks=all`).
Solo se activa con `KAFKA_ENABLED=true`. Para probarlo en tu computador sin AWS:

```bash
npm run up && npm run setup        # Kafka listo
npm run report                     # terminal 2: consumidor que verá los eventos
npm run pedido-real -- --cantidad=5   # terminal 3: crea 5 pedidos, actualiza 1 y elimina 1 con los handlers REALES
```

La prueba reemplaza DynamoDB y SNS por versiones en memoria (no toca tu cuenta de AWS). También puedes hacer los casos 1–4
mientras `pedido-real` genera tráfico en vez de `generar`.

## Si usas Windows (PowerShell)

PowerShell no acepta `VARIABLE=valor comando`. Define la variable y luego corre el comando:

```powershell
$env:PROCESS_DELAY_MS="100"; $env:PORT_REPORT="3004"; npm run report
$env:KAFKA_BROKERS="localhost:9092,localhost:9093,localhost:9094"; $env:REPLICATION_FACTOR="3"
```

`./zk.sh` se ejecuta en Git Bash. Kafka UI usa el puerto **8081** (el 8080 suele estar ocupado por XAMPP).

## Caso 1 · Escalar consumidores y ver el lag bajar (10 min)

```bash
npm run lag -- --grupo=report                          # A) monitor (déjalo abierto)
PROCESS_DELAY_MS=100 npm run report                    # B) consumidor lento
npm run generar -- --pedidos=500 --delay=50            # C) producir con ritmo
PROCESS_DELAY_MS=100 PORT_REPORT=3004 npm run report   # D) 2.º consumidor (y 3.º: 3005, 4.º: 3006)
```

1. El lag sube con 1 consumidor.  2. Con 2, baja más rápido.  3. Con 3, cada uno lee una partición.
4. Con 4, uno queda ocioso (regla de oro: consumidores útiles ≤ particiones).
En Kafka UI: *Consumers > report*. Luego `npm run particiones` (a 6) y observa que el 4.º ahora sí trabaja.

## Caso 2 · Se cae un broker y el clúster sigue (8 min)

```bash
docker compose down                                    # apaga el de 1 broker
docker compose -f docker-compose.cluster.yml up -d
export KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094
export REPLICATION_FACTOR=3
npm run setup
npm run replicas -- --watch                            # en vivo
docker stop cno-kafka-2                                # en otra terminal
./zk.sh                                                # controlador antes/después (KAFKA_CONTAINER=cno-kafka-1)
docker start cno-kafka-2                               # vuelve al ISR
```

Mientras tanto sigue produciendo (`npm run generar`): no se pierde nada porque usa `acks=all`.
**¿Cuántos brokers pueden caer con RF=3, acks=all y min.insync.replicas=2 y seguir escribiendo?** → **1**.

## Caso 3 · Ver cómo Kafka borra mensajes viejos (5 min)

```bash
npm run retencion
```

Crea un topic con `retention.ms=20 s` y segmentos chicos, envía 200 mensajes e imprime `low`/`high` cada 5 s.
`high` no cambia; `low` sube cuando Kafka borra segmentos cerrados (el broker revisa cada 30 s, configurado en el compose).
Los números varían: lo importante es que **low sube**. Kafka UI > Topics > `orders.demo-retencion` > Settings.

## Caso 4 · Tuning y prueba de carga (7 min)

```bash
npm run carga -- --count=20000 --modo=simple
npm run carga -- --count=20000 --modo=lote
npm run carga -- --count=20000 --modo=lote --lingerMs=0
npm run carga -- --count=20000 --modo=lote --acks=0
npm run carga -- --count=20000 --modo=lote --acks=-1
npm run carga -- --count=20000 --modo=lote --compresion=none
```

| Prueba | msg/s | llamadas |
|---|---|---|
| simple | ____ | ____ |
| lote | ____ | ____ |
| lote acks=0 | ____ | ____ |
| lote sin compresión | ____ | ____ |

Esperable: lote muy por sobre simple; `acks=0` más rápido que `acks=-1`; la compresión baja los bytes a costa de CPU.
Los números dependen de tu computador. `kafkajs` no trae `batch.size`/`linger.ms`, así que `carga.js` los emula con un
acumulador (`--batchSize`, `--lingerMs`), igual que el productor oficial de Java.

## Preguntas tipo parcial (con respuesta)

1. **3 particiones y 5 consumidores en el mismo grupo:** trabajan 3, 2 quedan ociosos.
2. **RF=3, acks=all, min.insync.replicas=2:** puede caer 1 broker y seguir escribiendo (con 2 caídos los datos existen, pero `acks=all` ya no acepta escrituras).
3. **Lag creciente sin parar:** el productor va más rápido que el consumidor. Soluciones: optimizar el consumidor, o más particiones + más consumidores.
4. **Retención de 7 días y 1 GB:** se aplica la que ocurra primero.
5. **Subir `linger.ms` de 0 a 50:** más latencia, mejor throughput (lotes más llenos).
6. **JSON inválido al DLT y sin reintento:** reintentar nunca lo arreglará; se aparta al DLT (`orders.events.dlt`) para revisarlo. Pruébalo: `npm run generar -- --pedidos=100 --invalidos=5` con un `report` corriendo.
7. **Quién elige al nuevo líder:** el controlador del clúster, que se registra en ZooKeeper (ver `../zookeeper`).

## Para la Prueba 3

Repite los casos y guarda capturas (Kafka UI, lag, ISR, retención), anota msg/s y llamadas, y explica con tus palabras
qué mejora y qué sacrifica cada parámetro.

## Apagar

```bash
npm run down
```
