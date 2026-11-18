# Informe Prueba 3 · Kafka en producción · CloudNativeOrders

**Alumno:** ______  **Asignatura:** DSY1107  **Fecha:** ______

> Completa cada sección con tus capturas y tus números. Las frases en cursiva son guía: bórralas y escribe con tus palabras.

## 1. Entorno
- Sistema operativo, versión de Docker y Node: ______
- Imágenes: cp-kafka 7.5.0, cp-zookeeper 7.5.0, kafka-ui 0.7.2
- Topics: `orders.events` (3 particiones), `orders.events.dlt`

## 2. Caso 1 · Escalamiento y consumer lag
| Consumidores | Qué observé en el lag | Particiones por consumidor |
|---|---|---|
| 1 | ______ | ______ |
| 2 | ______ | ______ |
| 3 | ______ | ______ |
| 4 | ______ | ______ |

📷 Capturas: terminal `npm run lag` y Kafka UI → Consumers → report.
*Explica: ¿por qué el 4.º consumidor queda ocioso? ¿Qué cambia al subir a 6 particiones?*

## 3. Caso 2 · Replicación y caída de un broker
📷 Capturas de `npm run replicas -- --watch`: antes, con el broker 2 apagado y después de reiniciarlo.
*Explica: qué es el ISR, quién eligió al nuevo líder y por qué no se perdieron pedidos (`acks=all`).*
*Responde: con RF=3, acks=all y min.insync.replicas=2, ¿cuántos brokers pueden caer y seguir escribiendo? (1)*

## 4. Caso 3 · Retención
📷 Capturas: tabla de `npm run retencion` y Kafka UI → Topics → `orders.demo-retencion` → Settings.
*Explica: qué significan `low` y `high`, por qué `low` subió y qué pasa si un consumidor estuvo caído más tiempo que la retención.*
Retenciones del proyecto: eventos 7 días / 512 MB · DLT 30 días.

## 5. Caso 4 · Tuning y pruebas de carga (experimentos 4.1 y 4.3)
| Prueba | msg/s | llamadas | tiempo (s) |
|---|---|---|---|
| simple | | | |
| lote | | | |
| lote `--lingerMs=0` | | | |
| lote `acks=0` | | | |
| lote `acks=-1` | | | |
| lote sin compresión | | | |

**Conclusión y decisión:** *¿Qué configuración elegirías para CloudNativeOrders y por qué? Piensa en negocio: notificar un pedido pide baja latencia; un reporte por hora pide throughput. Perder un pedido es peor que tardar milisegundos, por eso `acks=all`.*

## 6. Parámetros explicados con mis palabras
| Parámetro | Qué mejora | Qué sacrifica |
|---|---|---|
| `batch.size` | | |
| `linger.ms` | | |
| `compression.type` | | |
| `buffer.memory` | | |
| `acks` | | |
| `fetch.min.bytes` / `max.poll.records` | | |

## 7. DLT
*Explica por qué un JSON inválido va al DLT y no se reintenta.* 📷 Captura del mensaje ☠️ y de `orders.events.dlt` en Kafka UI.

## 8. ZooKeeper (si la prueba lo pide)
📷 `npm run estado` (1 LEADER + 2 FOLLOWER), `npm run controlador` y el árbol en ZooNavigator.

## 9. Conclusiones
______
