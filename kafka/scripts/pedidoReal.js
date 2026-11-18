// Prueba LOCAL del proyecto real (npm run pedido-real [-- --cantidad=5]).

const path = require('path');
const Module = require('module');
process.removeAllListeners('warning');

process.env.KAFKA_ENABLED = 'true';
process.env.KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';
process.env.TOPIC_ARN = process.env.TOPIC_ARN || 'arn:aws:sns:local:000000000000:pedidos';

const SRC = path.resolve(__dirname, '..', '..', 'src');


const tabla = new Map();
const dbFalsa = {
  TABLE_NAME: 'Orders-local', KEY_NAME: 'id',
  docClient: {
    async send(cmd) {
      const i = cmd.input, nombre = cmd.constructor.name, id = i.Key && i.Key.id;
      if (nombre === 'PutCommand') { tabla.set(i.Item.id, { ...i.Item }); return {}; }
      if (nombre === 'GetCommand') return { Item: tabla.get(id) };
      if (nombre === 'DeleteCommand') { tabla.delete(id); return {}; }
      if (nombre === 'UpdateCommand') {
        const item = tabla.get(id);
        for (const [n, campo] of Object.entries(i.ExpressionAttributeNames)) {
          item[campo] = i.ExpressionAttributeValues[n === '#updatedAt' ? ':updatedAt' : ':v' + n.slice(2)];
        }
        return { Attributes: { ...item } };
      }
      throw new Error('Comando no soportado en la prueba local: ' + nombre);
    },
  },
};
const snsFalso = { SNSClient: class { async send() { return {}; } }, PublishCommand: class { constructor(x) { this.input = x; } } };
const cargar = Module._load;
Module._load = function (req, parent, ...r) {
  if (req === '@aws-sdk/client-sns') return snsFalso;
  if (parent && parent.filename && parent.filename.startsWith(SRC) && /(^|\/)db$/.test(req)) return dbFalsa;
  return cargar.call(this, req, parent, ...r);
};

const crear = require(path.join(SRC, 'handlers', 'createOrder')).handler;
const actualizar = require(path.join(SRC, 'handlers', 'updateOrder')).handler;
const eliminar = require(path.join(SRC, 'handlers', 'deleteOrder')).handler;

const cantidad = Number((process.argv.find((a) => a.startsWith('--cantidad=')) || '--cantidad=5').split('=')[1]);
const evento = (body, id) => ({ body: body && JSON.stringify(body), pathParameters: id ? { id } : undefined });

(async () => {
  console.log(`Publicando en Kafka (${process.env.KAFKA_BROKERS}) usando los handlers reales...\n`);
  const ids = [];
  for (let n = 1; n <= cantidad; n++) {
    const r = await crear(evento({
      customerName: `Cliente ${n}`, customerEmail: `cliente${n}@correo.cl`,
      items: [{ productId: 'P-1', name: 'Producto 1', price: 1000 * n, quantity: 2 }],
    }));
    const pedido = JSON.parse(r.body);
    const o = pedido.data || pedido;      // por si response.js envuelve la respuesta
    ids.push(o.id);
    console.log(`PedidoCreado       -> HTTP ${r.statusCode} · ${o.id}`);
  }
  const u = await actualizar(evento({ status: 'confirmed' }, ids[0]));
  console.log(`PedidoActualizado  -> HTTP ${u.statusCode} · ${ids[0]} (status=confirmed)`);
  const d = await eliminar(evento(null, ids[ids.length - 1]));
  console.log(`PedidoEliminado    -> HTTP ${d.statusCode} · ${ids[ids.length - 1]}`);
  console.log('\nRevisa los eventos en Kafka UI (http://localhost:8081 > Topics > orders.events > Messages)');
  console.log('o en el consumidor: npm run report');
  setTimeout(() => process.exit(0), 500);
})().catch((e) => { console.error('Error:', e); process.exit(1); });
