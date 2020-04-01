#!/bin/sh

echo "Waiting for airflow to start"
while ! nc -z -v webserver 8080 < /dev/null > /dev/null 2>&1; do
  sleep 10
done

echo "Initiating Curl"

until $(curl --output /dev/null --silent --head --fail 'http://webserver:8080/admin/rest_api/api?api=unpause&dag_id=e2e'); do
    printf '.'
    sleep 5
done

until $(curl --output /dev/null --silent --head --fail 'http://webserver:8080/admin/rest_api/api?api=trigger_dag&dag_id=e2e'); do
    printf '.'
    sleep 5
done

echo "Done"
echo "Exiting container"
