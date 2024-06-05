#!/usr/bin/env sh
#
# NAME
#    bootstrap -- initialize/update docker environment

# vim: set ts=2 sts=2 sw=2 et:
set -e

TEST_HOST=${TEST_HOST:-"127.0.0.1"}

echo "Integration test host: ${TEST_HOST}"

get_exposed_port() {
  docker-compose port $1 $2 | cut -d: -f2
}

wait_for() {
  printf 'Waiting for %s... ' $1
    counter="0"
    while true
    do
      if [ "$( docker compose ps | grep $1 | grep -c healthy )" -eq 1 ]; then
        break
      fi
      counter=$((counter+1))
      if [ "${counter}" -eq 120 ]; then
        echo " ERROR: container failed to start"
        exit 1
      fi
      sleep 1
    done
    echo 'done.'
}

# Ensure Docker is Running
echo "Docker Information:"
echo ""
docker version
echo ""

# Activate the virtual environment
if test -e env/bin/activate
then
  . env/bin/activate
fi

mkdir -p build

# Stop any running instances and clean up after them, then pull images
docker-compose down --volumes --remove-orphans
docker-compose pull -q
docker-compose up -d

wait_for rabbitmq

docker-compose exec -T rabbitmq rabbitmqctl await_startup

cat > build/test-environment<<EOF
export RABBITMQ_URL=amqp://guest:guest@${TEST_HOST}:$(get_exposed_port rabbitmq 5672)/%2f
EOF
