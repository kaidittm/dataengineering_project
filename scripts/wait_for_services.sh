#!/bin/bash
set -euo pipefail

# Wait for a TCP host:port to be available using bash /dev/tcp
wait_for_tcp() {
  host=$1
  port=$2
  retries=${3:-60}
  delay=${4:-2}
  echo "Waiting for $host:$port"
  for i in $(seq 1 $retries); do
    if (echo > /dev/tcp/${host}/${port}) >/dev/null 2>&1; then
      echo "$host:$port is available"
      return 0
    fi
    echo "  attempt $i/$retries - $host:$port not available yet"
    sleep $delay
  done
  echo "Timed out waiting for $host:$port" >&2
  return 1
}

# Wait for ClickHouse HTTP endpoint using curl (more strict check)
wait_for_http() {
  url=$1
  retries=${2:-60}
  delay=${3:-2}
  echo "Waiting for HTTP service $url"
  for i in $(seq 1 $retries); do
    if curl --silent --fail "$url" >/dev/null 2>&1; then
      echo "$url is responding"
      return 0
    fi
    echo "  attempt $i/$retries - $url not responding yet"
    sleep $delay
  done
  echo "Timed out waiting for $url" >&2
  return 1
}

# Hosts from docker-compose
POSTGRES_HOST=airflow-db
POSTGRES_PORT=5432
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123

# Wait for Postgres TCP port
wait_for_tcp "$POSTGRES_HOST" "$POSTGRES_PORT" 60 2

# Wait for ClickHouse HTTP
wait_for_http "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" 60 2

exit 0
