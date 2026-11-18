# ¿Cómo funciona ZooKeeper? (DSY1107 · CloudNativeOrders)

ZooKeeper es un **servicio de coordinación** para sistemas distribuidos. No guarda datos de negocio:
guarda datos **pequeños** sobre la coordinación (quién está vivo, quién es el líder, cuál es la config vigente)
y avisa a los interesados cuando algo cambia.

Este ejemplo muestra sus piezas, una por script:

| Concepto | Qué es | Script |
|---|---|---|
| **Ensamble** | 3 servidores ZooKeeper; uno es *leader*, los otros *followers* | `ensamble/estado.js` |
| **Quórum** | Se necesita mayoría (2 de 3) para leer/escribir | `ensamble/estado.js` |
| **znodes** | El árbol de datos (como carpetas/archivos) | `znodes/tour.js`, `znodes/arbol.js` |
| **Sesión** | Conexión del cliente; si expira, se borran sus efímeros | todos |
| **Watches** | Avisos de un solo uso cuando un znode cambia | `config/`, `registro/`, `lider/` |
| **Versiones** | Escritura condicional (concurrencia optimista) | `znodes/tour.js`, `config/cambiar.js` |
| **Elección de líder** | Receta con efímeros secuenciales | `lider/candidato.js` |
| **Controlador de Kafka** | Cómo Kafka usa ZooKeeper para elegir su controlador | `kafka/controlador.js` |

## Arquitectura

```
          tu código Node (localhost:2181,2182,2183)        ZooNavigator :9000
                         │  una sesión, se mueve entre nodos si uno cae
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
     ┌───────┐  2888  ┌───────┐  2888  ┌───────┐
     │ zoo1  │◄──────►│ zoo2  │◄──────►│ zoo3  │     3888: elección de líder entre servidores
     │follower│       │LEADER │        │follower│
     └───────┘        └───────┘        └───────┘
         ▲  todas las ESCRITURAS pasan por el líder, que las replica;
         │  se confirman cuando la mayoría (2/3) las tiene. Las lecturas las atiende cualquiera.
         │
   kafka1 :9093 · kafka2 :9094   (perfil opcional "kafka": se registran en /brokers/ids y compiten por /controller)
```

## Estructura

```
zookeeper/
├─ docker-compose.yml      <- zoo1, zoo2, zoo3 + ZooNavigator  [+ kafka1, kafka2 con --profile kafka]
├─ shared/zk.js            <- cliente (ZK_CONNECT) + operaciones con async/await + describir Stat
├─ ensamble/estado.js      <- leader/follower, zxid y quórum de cada servidor (cada 2 s)
├─ znodes/
│   ├─ tour.js             <- recorrido guiado: persistente, efímero, secuencial, versión, watch
│   └─ arbol.js            <- imprime el árbol de znodes desde una ruta
├─ config/
│   ├─ servicio.js         <- servicio que lee su config y reacciona a cambios (watch)
│   └─ cambiar.js          <- cambia la config con escritura condicional por versión
├─ registro/
│   ├─ servicio.js         <- instancia que se registra con un znode efímero
│   └─ descubrir.js        <- cliente que ve las instancias vivas y les reparte pedidos
├─ lider/candidato.js      <- elección de líder (cierre de caja: solo uno lo ejecuta)
└─ kafka/controlador.js    <- muestra /controller, /controller_epoch y /brokers/ids de Kafka
```

Árbol que se va formando:

```
/
├─ zookeeper/                       <- interno de ZooKeeper (cuotas, config)
├─ cloudnativeorders/
│   ├─ config            [P]        <- {"descuento":15,...}       (config/)
│   ├─ servicios/report/
│   │   ├─ instancia-0000000000 [E] <- {"puerto":"4001",...}      (registro/)
│   │   └─ instancia-0000000001 [E]
│   ├─ eleccion/
│   │   ├─ c-0000000000  [E]  👑     <- el número más bajo manda  (lider/)
│   │   └─ c-0000000001  [E]
│   └─ demo/                         <- znodes/tour.js
├─ brokers/ids/1, /brokers/ids/2 [E] <- Kafka
├─ controller            [E]        <- {"brokerid":1,...}  Kafka
└─ controller_epoch      [P]        <- 3
```

## Paso a paso

Requisitos: Docker Desktop y Node 18+.

```bash
npm install
docker compose up -d        # o: npm run up       (agrega --profile kafka para la parte 6)
docker compose ps           # zk-zoo1, zk-zoo2, zk-zoo3 y zk-ui en "running"
```

ZooNavigator: http://localhost:9000 (se conecta solo al ensamble; ahí se ve el árbol en vivo).

> Puede convivir con la carpeta `../kafka` (casos prácticos): usa otros nombres de contenedor y otros puertos
> (2181-2183, 9000, 9093-9094). Si `../kafka` ya está usando 2181 hacia el host, apágalo primero.

### 1. El ensamble y el quórum

```bash
npm run estado              # deja esta terminal abierta
```

| # | Haz esto | Deberías ver |
|---|----------|--------------|
| 1 | Mira la tabla | Un `LEADER` y dos `FOLLOWER`, todos con el mismo `zxid` |
| 2 | `docker stop <contenedor del LEADER>` | En segundos otro nodo pasa a `LEADER`. El **epoch** del zxid sube (ej. `0x2…` → `0x300000000`) |
| 3 | Detén un segundo nodo | El que queda dice `SIN QUÓRUM`: con 1 de 3 no hay mayoría y **no atiende a nadie** (evita *split brain*) |
| 4 | `docker start zk-zoo1 zk-zoo2 zk-zoo3` | Vuelven, se sincronizan con el líder y el servicio se recupera |

Pregunta para la clase: ¿por qué se usan ensambles de 3 o 5 y no de 4? (4 tolera 1 caída, igual que 3.)

### 2. znodes, sesiones, versiones y watches

```bash
npm run znodes              # recorrido guiado de 8 pasos
npm run arbol -- /cloudnativeorders/demo   # en otra terminal, mientras el tour espera 30 s
npm run arbol -- /cloudnativeorders/demo   # otra vez cuando el tour termine: los [E] desaparecieron
```

Tipos de znode:
- **Persistente**: queda hasta que alguien lo borre.
- **Efímero**: ligado a la sesión que lo creó. Si la sesión termina o expira, ZooKeeper lo borra. No puede tener hijos.
- **Secuencial**: ZooKeeper agrega un contador único y creciente al nombre (`turno-0000000003`).

### 3. Configuración centralizada (watch sobre el dato)

```bash
npm run config -- report    # terminal 1
npm run config -- audit     # terminal 2
npm run config:set -- '{"descuento":15}'          # terminal 3
npm run config:set -- '{"modoMantencion":true}'
```

Ambos servicios cambian su comportamiento al instante, sin reiniciar.
Fíjate en el patrón: el watch es de **un solo uso**, por eso cada aviso vuelve a leer y a registrar el watch.

### 4. Registro y descubrimiento de servicios (efímeros + watch sobre hijos)

```bash
npm run descubrir -- report        # terminal 1: el "gateway" que reparte pedidos
npm run servicio -- report 4001    # terminal 2
npm run servicio -- report 4002    # terminal 3
```

| # | Haz esto | Deberías ver |
|---|----------|--------------|
| 1 | Levanta las 2 instancias | El gateway reparte pedidos 4001, 4002, 4001… |
| 2 | Ctrl+C en una instancia | Desaparece **al instante** (cerró su sesión) |
| 3 | `kill -9 <pid>` en la otra | Desaparece **~6 s después**: ZooKeeper esperó el `sessionTimeout` para declararla muerta |

### 5. Elección de líder

```bash
npm run candidato -- A      # terminal 1
npm run candidato -- B      # terminal 2
npm run candidato -- C      # terminal 3
```

| # | Haz esto | Deberías ver |
|---|----------|--------------|
| 1 | Levanta A, B y C | A es líder y ejecuta el cierre de caja. B vigila a A, C vigila a **B** (no al líder) |
| 2 | Ctrl+C en A | **Solo B** recibe el aviso y pasa a líder. C ni se entera (no hay efecto manada) |
| 3 | `kill -9` al líder | Igual, pero tras expirar la sesión |
| 4 | Ctrl+C en B (seguidor del medio) | C ahora vigila a A… o pasa a líder si B era el líder |

Esta misma receta sirve para un **lock distribuido**: "ser líder" = "tener el lock".

### 6. ZooKeeper dentro de Kafka: el controlador

```bash
docker compose --profile kafka up -d     # o: npm run up:kafka   (esperar ~20 s)
npm run controlador
npm run arbol -- /brokers
```

| # | Haz esto | Deberías ver |
|---|----------|--------------|
| 1 | Mira la salida | 2 brokers en `/brokers/ids` (efímeros) y uno de ellos es el controlador |
| 2 | `docker stop zk-kafka<id del controlador>` | `/controller` se borra, el otro broker lo crea y `controller_epoch` sube |
| 3 | `docker start` del mismo | Reaparece en `/brokers/ids`, pero **no** recupera el rol: el controlador actual sigue |

Es exactamente la receta de los pasos 4 y 5: Kafka usa efímeros para saber qué brokers viven
y un efímero único (`/controller`) para elegir al coordinador del clúster.
Kafka moderno (KRaft, 3.3+) ya no necesita ZooKeeper: integra esa coordinación con un quórum Raft propio,
pero los conceptos (líder elegido, epoch, quórum) son los mismos.

## Consola de ZooKeeper (zkCli)

```bash
docker exec -it zk-zoo1 zkCli.sh
ls /                            # hijos de la raíz
get -s /cloudnativeorders/config       # dato + stat (version, ephemeralOwner, zxid...)
create -e /yo-temporal "hola"   # efímero: sal con quit y desaparece
create -s /cloudnativeorders/t- ""     # secuencial
get -w /cloudnativeorders/config       # lee y deja un watch: cámbiala desde otra terminal
```

Estado de cada servidor: `docker exec zk-zoo1 zkServer.sh status`

## Apagar

```bash
npm run down                # baja ZooKeeper, la UI y los brokers (si estaban)
```
