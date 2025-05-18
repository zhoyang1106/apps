#!/bin/bash

# restart redis container
container_name="my-redis"

echo "=> Restart $container_name container..."

echo "=> $(sudo docker exec -it $container_name redis-cli flushall)" && \
echo "=> $(sudo docker restart $container_name)"

echo "=> $container_name has been restarted."


cd /home/pi/apps
