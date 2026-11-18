// Publica eventos de pedidos en Kafka (DSY1107).
//
// - Está APAGADO por defecto: solo funciona si KAFKA_ENABLED=true. Así el despliegue en AWS Lambda no cambia.
// - "kafkajs" se carga solo cuando está encendido (require perezoso).
// - Nunca lanza errores: si Kafka no responde, el pedido igual se crea/actualiza/elimina (igual que con SNS).
// - Mismo formato de evento que ya usa SNS + el pedido completo, para que lo lean los consumidores de kafka/.
//
// Variables de entorno:
//   KAFKA_ENABLED=true            enciende la publicación
//   KAFKA_BROKERS=localhost:9092  lista separada por comas (por defecto localhost:9092)
//   KAFKA_TOPIC=orders.events     topic de destino (por defecto orders.events)
const { v4: uuidv4 } = require('uuid');

let producerPromise = null;

function getProducer() {
  if (!producerPromise) {
    const { Kafka, logLevel } = require('kafkajs');
    const kafka = new Kafka({
      clientId: 'cloudnativeorders-api',
      brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
      logLevel: logLevel.NOTHING,
      connectionTimeout: 3000,
      retry: { retries: 1 },
    });
    const producer = kafka.producer();
    producerPromise = producer.connect().then(() => producer).catch((e) => {
      producerPromise = null;           // permite reintentar en la próxima invocación
      throw e;
    });
  }
  return producerPromise;
}

/**
 * @param {string} tipoEvento  "PedidoCreado" | "PedidoActualizado" | "PedidoEliminado"
 * @param {object} order       el pedido (la clave del mensaje es order.id)
 * @returns {Promise<boolean>} true si se publicó; false si estaba apagado o falló
 */
async function publicarEventoKafka(tipoEvento, order) {
  if (process.env.KAFKA_ENABLED !== 'true') return false;
  try {
    const producer = await getProducer();
    await producer.send({
      topic: process.env.KAFKA_TOPIC || 'orders.events',
      acks: -1,   // acks=all: perder un pedido es peor que tardar unos milisegundos
      messages: [{
        key: String(order.id),   // misma clave -> misma partición -> eventos de un pedido en orden
        value: JSON.stringify({
          type: tipoEvento,
          eventId: uuidv4(),
          timestamp: new Date().toISOString(),
          pedidoId: order.id,
          order,
        }),
      }],
    });
    return true;
  } catch (e) {
    console.error(`No se pudo publicar ${tipoEvento} en Kafka:`, e.message);
    return false;
  }
}

module.exports = { publicarEventoKafka };
