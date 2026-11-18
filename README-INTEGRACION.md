# Cómo integrar estos archivos en CloudNativeOrders

Copia el contenido del zip a la raíz del repo, aceptando "reemplazar":

```
CloudNativeOrders/
├─ src/
│   ├─ utils/kafkaPublisher.js     <- NUEVO: publica eventos en Kafka (apagado por defecto)
│   └─ handlers/
│       ├─ createOrder.js          <- MODIFICADO: +2 líneas (publica PedidoCreado)
│       ├─ updateOrder.js          <- MODIFICADO: +2 líneas (publica PedidoActualizado)
│       └─ deleteOrder.js          <- MODIFICADO: +2 líneas (publica PedidoEliminado)
├─ package.json                    <- MODIFICADO: agrega la dependencia "kafkajs"
├─ kafka/                          <- NUEVO: casos prácticos + prueba local del proyecto real
└─ zookeeper/                      <- NUEVO: demo de ZooKeeper
```

- Después de copiar, en la raíz del repo: `npm install` (instala kafkajs). Luego en `kafka/` y `zookeeper/`: `npm install`.
- Kafka en el proyecto está **apagado por defecto** (`KAFKA_ENABLED` no definido): en AWS todo sigue igual que antes.
- Para encenderlo: `KAFKA_ENABLED=true` y `KAFKA_BROKERS=localhost:9092`. Si Kafka no responde, el pedido igual se crea (se imprime el error en consola).
- Prueba local sin AWS: `cd kafka && npm run pedido-real`.
- Agrega `node_modules/` al `.gitignore`.
- Puertos: `kafka/` usa 9092 (y 9093/9094 en el clúster) + 8081; `zookeeper/` usa 2181-2183 + 9000. No levantes a la vez el clúster de `kafka/` y el perfil `--profile kafka` de `zookeeper/`.
