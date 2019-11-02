#!/bin/sh
set -x

echo "Preparing zookeeper, kafka hosts"

sed -i "s|ZOOKEEPER_HOSTS|$ZOOKEEPER_HOSTS|g" /etc/burrow/burrow.toml
sed -i "s|KAFKA_HOSTS|$KAFKA_HOSTS|g" /etc/burrow/burrow.toml
sed -i "s|ENV|$ENV|g" /etc/burrow/burrow.toml

sed -i "s|METRICS_KAFKA_HOST|$METRICS_KAFKA_HOST|g" /app/config/config.json
sed -i "s|METRICS_TOPIC|$METRICS_TOPIC|g" /app/config/config.json

echo "Start rainbow server in backgrond"
chmod +x /app/rainbow
/app/rainbow &


exec "$@"
