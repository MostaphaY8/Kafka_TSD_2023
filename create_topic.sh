docker compose exec broker \
  kafka-topics --create \
    --topic purchases \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1

    docker compose exec broker \
  kafka-topics --create \
    --topic grades \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1

    docker compose exec broker \
  kafka-topics --create \
    --topic message \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1