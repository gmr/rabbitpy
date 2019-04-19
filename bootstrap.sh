#!/usr/bin/env bash
#
# NAME
#    bootstrap -- initialize/update docker environment

# vim: set ts=2 sts=2 sw=2 et:
set -e

# Common constants
COLOR_RESET='\033[0m'
COLOR_GREEN='\033[0;32m'
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-${PWD##*/}}"
TEST_HOST="${TEST_HOST:-localhost}"

echo "Integration test host: ${TEST_HOST}"

get_ipaddr() {
  docker inspect --format '{{ .NetworkSettings.IPAddress }}' $1
}

get_exposed_port() {
  docker-compose port $1 $2 | cut -d: -f2
}

report_start() {
  printf "Waiting for $1 ... "
}

report_done() {
  printf "${COLOR_GREEN}done${COLOR_RESET}\n"
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

mkdir -p build reports ddl/build

# Stop any running instances and clean up after them, then pull images
docker-compose down --volumes --remove-orphans
docker-compose pull -q
docker-compose up -d

report_start "RabbitMQ"
sleep 10
docker-compose exec -T rabbitmq rabbitmqctl await_startup
report_done

cat > build/test-environment<<EOF
export RABBITMQ_URL=amqp://guest:guest@${TEST_HOST}:$(get_exposed_port rabbitmq 5672)/%2f
EOF

printf "\nBootstrap complete\n\nDon't forget to \". build/test-environment\"\n"
