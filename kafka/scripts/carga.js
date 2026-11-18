// Caso 4: prueba de carga del productor (npm run carga -- --count=20000 --modo=lote ...)

const zlib = require('zlib');
const { kafka, args, pedidoEjemplo, eventoPedido, TOPIC_CARGA, REPLICATION_FACTOR } = require('../shared/kafka');
const { CompressionTypes } = require('kafkajs');

(async () => {
  const a = args();
  const count = Number(a.count || 20000);
  const modo = a.modo || 'lote';
  const batchSize = Number(a.batchSize || 500);
  const lingerMs = Number(a.lingerMs === undefined ? 20 : a.lingerMs);
  const acks = Number(a.acks === undefined ? -1 : a.acks);
  const compresion = (a.compresion || 'gzip') === 'none' ? CompressionTypes.None : CompressionTypes.GZIP;

  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ waitForLeaders: true, topics: [{ topic: TOPIC_CARGA, numPartitions: 3, replicationFactor: REPLICATION_FACTOR }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  
  const msgs = Array.from({ length: count }, () => { const o = pedidoEjemplo(); return { key: o.id, value: JSON.stringify(eventoPedido(o)) }; });
  const bytes = msgs.reduce((s, m) => s + Buffer.byteLength(m.value), 0);
  const gz = zlib.gzipSync(Buffer.from(msgs.slice(0, Math.min(batchSize, count)).map((m) => m.value).join('\n'))).length;
  const ratio = gz / msgs.slice(0, Math.min(batchSize, count)).reduce((s, m) => s + Buffer.byteLength(m.value) + 1, 0);

  let llamadas = 0;
  const enVuelo = new Set();
  const enviar = (lote) => {
    llamadas++;
    const pr = producer.send({ topic: TOPIC_CARGA, acks, compression: compresion, messages: lote }).finally(() => enVuelo.delete(pr));
    enVuelo.add(pr);
    return pr;
  };

  const t0 = process.hrtime.bigint();
  if (modo === 'simple') {
    for (const m of msgs) await enviar([m]);
  } else {
    let buffer = [], timer = null;
    const vaciar = () => { clearTimeout(timer); timer = null; if (buffer.length) { const l = buffer; buffer = []; enviar(l); } };
    for (const m of msgs) {
      buffer.push(m);
      if (buffer.length >= batchSize || lingerMs === 0) vaciar();          // lote lleno (o sin espera)
      else if (!timer) timer = setTimeout(vaciar, lingerMs);               // espera linger.ms a que se llene
      if (enVuelo.size >= 50) await Promise.race(enVuelo);                 // límite de envíos simultáneos
    }
    vaciar();
    await Promise.all(enVuelo);
  }
  const seg = Number(process.hrtime.bigint() - t0) / 1e9;
  await producer.disconnect();

  const nombreAcks = acks === -1 ? 'all' : String(acks);
  console.log(`modo=${modo} acks=${nombreAcks} compresión=${compresion === CompressionTypes.None ? 'none' : 'gzip'}` + (modo === 'lote' ? ` batchSize=${batchSize} lingerMs=${lingerMs}` : ''));
  console.log(`  mensajes : ${count}`);
  console.log(`  tiempo   : ${seg.toFixed(2)} s`);
  console.log(`  msg/s    : ${Math.round(count / seg)}`);
  console.log(`  llamadas : ${llamadas}`);
  console.log(`  bytes    : ${(bytes / 1024).toFixed(0)} KB sin comprimir` + (compresion !== CompressionTypes.None ? ` (≈ ${(bytes * ratio / 1024).toFixed(0)} KB con gzip, ${(ratio * 100).toFixed(0)}%)` : ''));
})().catch((e) => { console.error('Error:', e.message); process.exit(1); });
