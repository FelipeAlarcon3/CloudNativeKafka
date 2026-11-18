// Crea los topics del proyecto (npm run setup). Es idempotente: se puede correr varias veces.
//   Para el clúster de 3 brokers:
//     KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094 REPLICATION_FACTOR=3 npm run setup
const { kafka, BROKERS, REPLICATION_FACTOR, TOPIC_EVENTOS, TOPIC_DLT } = require('../shared/kafka');

(async () => {
  const admin = kafka.admin();
  await admin.connect();
  const rf = REPLICATION_FACTOR;
  const minIsr = rf >= 3 ? '2' : '1';   

  const creados = await admin.createTopics({
    waitForLeaders: true,
    topics: [
      {   
        topic: TOPIC_EVENTOS, numPartitions: 3, replicationFactor: rf,
        configEntries: [
          { name: 'retention.ms', value: String(7 * 24 * 3600 * 1000) },
          { name: 'retention.bytes', value: String(512 * 1024 * 1024) },
          { name: 'min.insync.replicas', value: minIsr },
        ],
      },
      {   
        topic: TOPIC_DLT, numPartitions: 1, replicationFactor: rf,
        configEntries: [
          { name: 'retention.ms', value: String(30 * 24 * 3600 * 1000) },
          { name: 'min.insync.replicas', value: minIsr },
        ],
      },
    ],
  });
  console.log(`Brokers: ${BROKERS.join(', ')} · RF=${rf} · min.insync.replicas=${minIsr}`);
  console.log(creados ? 'Topics creados.' : 'Los topics ya existían (sin cambios).');
  await admin.disconnect();
})().catch((e) => { console.error('Error:', e.message); process.exit(1); });
