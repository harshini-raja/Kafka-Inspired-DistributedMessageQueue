#!/bin/sh

HOST="$1"
PORT="$2"
shift 2

while ! nc -z "$HOST" "$PORT"; do
  echo "Waiting for $HOST:$PORT..."
  sleep 2
done

echo "$HOST:$PORT is available - executing command"
exec "$@"