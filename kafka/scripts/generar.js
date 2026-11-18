//   --pedidos   cuántos eventos "PedidoCreado" enviar (por defecto 100)
//   --delay     ms de espera entre mensajes (por defecto 0)
//   --invalidos cuántos mensajes con JSON roto mezclar (van al DLT en el consumidor)
// acks=all (-1): en CloudNativeOrders perder un pedido es peor que tardar unos milisegundos más.
const { kafka, args, esperar, pedidoEjemplo, eventoPedido, TOPIC_EVENTOS } = require('../shared/kafka');

(async () => {
  const a = args();
  const total = Number(a.pedidos || 100);
  const delay = Number(a.delay || 0);
  const invalidos = Number(a.invalidos || 0);

  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();
  console.log(`Enviando ${total} pedidos a ${TOPIC_EVENTOS} (acks=all, delay=${delay} ms)...`);

  const t0 = Date.now();
  for (let i = 1; i <= total; i++) {
    const order = pedidoEjemplo();
    const esInvalido = invalidos > 0 && i % Math.max(1, Math.floor(total / invalidos)) === 0;
    await producer.send({
      topic: TOPIC_EVENTOS,
      acks: -1,
      
      messages: [{ key: order.id, value: esInvalido ? '{esto no es json' : JSON.stringify(eventoPedido(order)) }],
    });
    if (i % 100 === 0 || i === total) process.stdout.write(`\r  ${i}/${total}`);
    if (delay) await esperar(delay);
  }
  console.log(`\nListo en ${((Date.now() - t0) / 1000).toFixed(1)} s.`);
  await producer.disconnect();
})().catch((e) => { console.error('Error:', e.message); process.exit(1); });
