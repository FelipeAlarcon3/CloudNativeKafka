// Muestra el estado de cada nodo del ensamble cada 2 s (npm run estado).
// Usa el comando de diagnóstico "srvr" (four-letter word) por TCP, igual que
//   docker exec zk-zoo1 zkServer.sh status
//
// Prueba: detén el líder (docker stop zk-zooX) y mira cómo los otros eligen uno nuevo.
const net = require('net');

const NODOS = [
  { nombre: 'zoo1', puerto: 2181, contenedor: 'zk-zoo1' },
  { nombre: 'zoo2', puerto: 2182, contenedor: 'zk-zoo2' },
  { nombre: 'zoo3', puerto: 2183, contenedor: 'zk-zoo3' },
];

function srvr(puerto) {
  return new Promise((resolve) => {
    let salida = '';
    const s = net.connect({ host: 'localhost', port: puerto }, () => s.write('srvr'));
    s.setTimeout(1500, () => { s.destroy(); resolve(null); });
    s.on('data', d => { salida += d; });
    s.on('end', () => resolve(salida));
    s.on('error', () => resolve(null));
  });
}

function parsear(texto) {
  if (!texto) return { modo: 'CAÍDO / sin respuesta' };
  if (texto.includes('not currently serving')) return { modo: 'SIN QUÓRUM (no atiende clientes)' };
  const campo = (k) => (texto.match(new RegExp(`${k}: (.*)`)) || [])[1];
  return {
    modo: (campo('Mode') || '?').toUpperCase(),   // LEADER / FOLLOWER
    zxid: campo('Zxid'),                          // última transacción aplicada (epoch + contador)
    znodes: campo('Node count'),
    conexiones: campo('Connections'),
  };
}

async function ciclo() {
  const filas = await Promise.all(NODOS.map(async n => ({ nodo: n.nombre, contenedor: n.contenedor, ...parsear(await srvr(n.puerto)) })));
  const vivos = filas.filter(f => f.modo === 'LEADER' || f.modo === 'FOLLOWER').length;
  console.clear();
  console.log(`Ensamble ZooKeeper  ·  ${new Date().toLocaleTimeString()}`);
  console.table(filas);
  console.log(`Nodos sirviendo: ${vivos}/3  ·  quórum necesario: 2  ·  ${vivos >= 2 ? '✅ HAY QUÓRUM' : '❌ SIN QUÓRUM: nadie puede leer ni escribir'}`);
  console.log('\nzxid = 0x<epoch><contador>: el epoch (primeros dígitos) sube cada vez que se elige un líder nuevo.');
  console.log('Prueba:  docker stop <contenedor del LEADER>   ...y luego   docker start <el mismo>');
}

ciclo();
setInterval(ciclo, 2000);
