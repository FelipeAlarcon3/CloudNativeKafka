// Aumenta las particiones de orders.events (npm run particiones [-- --total=6]).

const { kafka, args, TOPIC_EVENTOS } = require('../shared/kafka');

(async () => {
  const total = Number(args().total || 6);
  const admin = kafka.admin();
  await admin.connect();
  const [{ partitions }] = (await admin.fetchTopicMetadata({ topics: [TOPIC_EVENTOS] })).topics;
  console.log(`${TOPIC_EVENTOS}: ${partitions.length} particiones actuales.`);
  if (total <= partitions.length) {
    console.log(`No se puede bajar (ni mantener) a ${total}: solo se pueden aumentar.`);
  } else {
    await admin.createPartitions({ topicPartitions: [{ topic: TOPIC_EVENTOS, count: total }] });
    console.log(`Ahora tiene ${total}. Los consumidores del grupo se rebalancean solos.`);
  }
  await admin.disconnect();
})().catch((e) => { console.error('Error:', e.message); process.exit(1); });
