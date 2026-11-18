// Consumidor "report" (grupo "report"): arma el reporte de pedidos.

const http = require('http');
const { kafka, esperar, TOPIC_EVENTOS, TOPIC_DLT } = require('../shared/kafka');

const GRUPO = process.env.GROUP_ID || 'report';
const DELAY = Number(process.env.PROCESS_DELAY_MS || 0);
const PORT = Number(process.env.PORT_REPORT || 3003);

(async () => {
  const consumer = kafka.consumer({ groupId: GRUPO, sessionTimeout: Number(process.env.SESSION_TIMEOUT_MS || 10000) });
  const producerDlt = kafka.producer();
  await consumer.connect();
  await producerDlt.connect();
  await consumer.subscribe({ topic: TOPIC_EVENTOS, fromBeginning: true });

  let procesados = 0, enDlt = 0, asignadas = [];
  consumer.on(consumer.events.GROUP_JOIN, (e) => {
    asignadas = e.payload.memberAssignment[TOPIC_EVENTOS] || [];
    console.log(`[${GRUPO}] rebalanceo -> mis particiones: [${asignadas.join(', ') || 'ninguna (ocioso)'}]`);
  });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      let evento;
      try {
        evento = JSON.parse(message.value.toString());
      } catch {
        enDlt++;
        await producerDlt.send({
          topic: TOPIC_DLT, acks: -1,
          messages: [{ key: message.key, value: message.value, headers: { motivo: 'JSON inválido', origen: `${TOPIC_EVENTOS}-${partition}` } }],
        });
        console.log(`[${GRUPO}] ☠️  p${partition} offset ${message.offset}: JSON inválido -> DLT (sin reintento)`);
        return;
      }
      if (DELAY) await esperar(DELAY);   // simula procesamiento lento
      procesados++;
      if (procesados % 20 === 0) console.log(`[${GRUPO}] p${partition} procesados=${procesados} dlt=${enDlt} (último pedido ${evento.pedidoId})`);
    },
  });

  http.createServer((_, res) => {
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ grupo: GRUPO, particiones: asignadas, procesados, dlt: enDlt }));
  }).listen(PORT, () => console.log(`[${GRUPO}] consumidor listo (delay=${DELAY} ms, /health en :${PORT})`));

  const salir = async () => { await consumer.disconnect(); await producerDlt.disconnect(); process.exit(0); };
  process.on('SIGINT', salir); process.on('SIGTERM', salir);
})().catch((e) => { console.error('Error:', e.message); process.exit(1); });
