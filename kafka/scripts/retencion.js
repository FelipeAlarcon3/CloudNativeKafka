// Caso 3: ver cómo Kafka borra mensajes viejos (npm run retencion).

const { kafka, args, esperar, pedidoEjemplo, eventoPedido, TOPIC_RETENCION, REPLICATION_FACTOR } = require('../shared/kafka');

(async () => {
  const duracion = Number(args().segundos || 90);
  const admin = kafka.admin();
  await admin.connect();
  await admin.deleteTopics({ topics: [TOPIC_RETENCION] }).catch(() => {});
  await esperar(2000);
  await admin.createTopics({
    waitForLeaders: true,
    topics: [{
      topic: TOPIC_RETENCION, numPartitions: 1, replicationFactor: REPLICATION_FACTOR,
      configEntries: [
        { name: 'retention.ms', value: '20000' },   // guarda 20 s
        { name: 'segment.bytes', value: '2048' },   // segmentos de 2 KB: se "cierran" rápido y se pueden borrar
        { name: 'segment.ms', value: '10000' },
      ],
    }],
  });

  const producer = kafka.producer();
  await producer.connect();
  for (let i = 0; i < 200; i++) {
    await producer.send({ topic: TOPIC_RETENCION, messages: [{ value: JSON.stringify(eventoPedido(pedidoEjemplo())) }] });
  }
  await producer.disconnect();
  console.log('200 mensajes enviados a', TOPIC_RETENCION, '(retention.ms=20000)\n');
  console.log('segundos   low   high  disponibles');

  const t0 = Date.now();
  for (;;) {
    const [{ low, high }] = await admin.fetchTopicOffsets(TOPIC_RETENCION);
    const seg = Math.round((Date.now() - t0) / 1000);
    console.log(`${String(seg).padStart(6)}  ${String(low).padStart(5)} ${String(high).padStart(6)}  ${String(high - low).padStart(10)}` + (Number(low) > 0 ? `   <- se borraron ${low}` : ''));
    if (seg >= duracion) break;
    await esperar(5000);
  }
  console.log('\nPrueba: un consumidor nuevo con fromBeginning solo verá desde el offset "low".');
  await admin.disconnect();
})().catch((e) => { console.error('Error:', e.message); process.exit(1); });
