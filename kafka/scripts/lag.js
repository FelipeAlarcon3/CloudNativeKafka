// Monitor de consumer lag (npm run lag -- --grupo=report). Déjalo abierto durante el caso 1.

const { kafka, args, esperar, TOPIC_EVENTOS } = require('../shared/kafka');

(async () => {
  const grupo = args().grupo || 'report';
  const admin = kafka.admin();
  await admin.connect();
  let anterior = null;
  console.log(`Lag del grupo "${grupo}" en ${TOPIC_EVENTOS} (Ctrl+C para salir)\n`);

  for (;;) {
    const fin = await admin.fetchTopicOffsets(TOPIC_EVENTOS);
    const [{ partitions: confirmados }] = await admin.fetchOffsets({ groupId: grupo, topics: [TOPIC_EVENTOS] });
    const filas = fin.map((p) => {
      const c = confirmados.find((x) => Number(x.partition) === Number(p.partition));
      const off = c && Number(c.offset) >= 0 ? Number(c.offset) : Number(p.low);
      return { p: Number(p.partition), fin: Number(p.offset), cons: off, lag: Number(p.offset) - off };
    });
    const total = filas.reduce((s, f) => s + f.lag, 0);
    const tendencia = anterior === null ? '' : total > anterior ? '  ⬆ sube' : total < anterior ? '  ⬇ baja' : '  = quieto';
    console.log(`${new Date().toLocaleTimeString()}  ` + filas.map((f) => `p${f.p}: ${String(f.lag).padStart(5)}`).join('  ') + `   | TOTAL ${String(total).padStart(5)}${tendencia}`);
    anterior = total;
    await esperar(2000);
  }
})().catch((e) => { console.error('Error:', e.message); process.exit(1); });
