
process.removeAllListeners('warning');

const { Kafka, logLevel } = require('kafkajs');

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const REPLICATION_FACTOR = Number(process.env.REPLICATION_FACTOR || 1);


const TOPIC_EVENTOS = 'orders.events';          // eventos de pedidos (3 particiones)
const TOPIC_DLT = 'orders.events.dlt';          // mensajes inválidos (Dead Letter Topic)
const TOPIC_RETENCION = 'orders.demo-retencion';// solo para el caso 3
const TOPIC_CARGA = 'orders.demo-carga';        // solo para el caso 4

const kafka = new Kafka({
  clientId: 'cloudnativeorders',
  brokers: BROKERS,
  logLevel: logLevel.NOTHING,
  retry: { retries: 8 },
});


function args() {
  const out = {};
  for (const a of process.argv.slice(2)) {
    const m = a.match(/^--([^=]+)(?:=(.*))?$/);
    if (m) out[m[1]] = m[2] === undefined ? true : m[2];
  }
  return out;
}

const esperar = (ms) => new Promise((r) => setTimeout(r, ms));


let n = 0;
function pedidoEjemplo() {
  n += 1;
  const items = Array.from({ length: 1 + (n % 3) }, (_, i) => ({
    productId: `P-${100 + ((n + i) % 20)}`,
    name: `Producto ${100 + ((n + i) % 20)}`,
    price: 1000 + ((n * 37 + i * 11) % 9000),
    quantity: 1 + ((n + i) % 4),
  }));
  const total = items.reduce((s, it) => s + it.price * it.quantity, 0);
  const now = new Date().toISOString();
  return {
    id: `${Date.now().toString(36)}-${n}-${Math.random().toString(36).slice(2, 8)}`,
    customerName: `Cliente ${n % 50}`,
    customerEmail: `cliente${n % 50}@correo.cl`,
    items, total, status: 'pending', createdAt: now, updatedAt: now,
  };
}

function eventoPedido(order) {
  return {
    type: 'PedidoCreado',
    eventId: `${order.id}-evt`,
    timestamp: new Date().toISOString(),
    pedidoId: order.id,
    order,
  };
}

module.exports = {
  kafka, BROKERS, REPLICATION_FACTOR, args, esperar, pedidoEjemplo, eventoPedido,
  TOPIC_EVENTOS, TOPIC_DLT, TOPIC_RETENCION, TOPIC_CARGA,
};
