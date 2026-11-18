// Muestra líder, réplicas e ISR de cada partición (npm run replicas [-- --watch]).
// Caso 2: apaga un broker (docker stop cno-kafka-2) y mira cómo cambian líder e ISR.
const { kafka, args, esperar, TOPIC_EVENTOS } = require('../shared/kafka');

async function mostrar(admin) {
  const { topics } = await admin.fetchTopicMetadata({ topics: [TOPIC_EVENTOS] });
  console.log(`\n── ${new Date().toLocaleTimeString()} ── ${TOPIC_EVENTOS}`);
  console.log('partición  líder  réplicas   ISR        estado');
  for (const p of topics[0].partitions) {
    const completo = p.isr.length === p.replicas.length;
    console.log(`    ${p.partitionId}       ${p.leader}     [${p.replicas.join(',')}]`.padEnd(32) + `[${p.isr.join(',')}]`.padEnd(11) + (completo ? 'ok' : '⚠ réplicas fuera del ISR'));
  }
}

(async () => {
  const admin = kafka.admin();
  await admin.connect();
  const watch = Boolean(args().watch);
  do {
    try { await mostrar(admin); } catch (e) { console.log(`(sin respuesta: ${e.message})`); }
    if (watch) await esperar(3000);
  } while (watch);
  await admin.disconnect();
})().catch((e) => { console.error('Error:', e.message); process.exit(1); });
