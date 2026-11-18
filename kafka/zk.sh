
set -e
C="${KAFKA_CONTAINER:-cno-kafka-1}"
Z="${ZK_CONNECT:-zookeeper:2181}"
echo "== /controller (broker controlador) =="
docker exec "$C" zookeeper-shell "$Z" get /controller 2>/dev/null | grep brokerid || echo "(sin controlador)"
echo "== /controller_epoch =="
docker exec "$C" zookeeper-shell "$Z" get /controller_epoch 2>/dev/null | tail -n +1 | grep -E '^[0-9]+$' || true
echo "== /brokers/ids (brokers vivos) =="
docker exec "$C" zookeeper-shell "$Z" ls /brokers/ids 2>/dev/null | tail -1
