#!/bin/bash

service_name="node-service-1"

# find serivce 
if sudo docker service ls | grep -q "$service_name"; then
    # try remove service
    if sudo docker service rm "$service_name"; then
        echo "Service '$service_name' remove successfully. Waiting for all remove..."

        # check service
        while sudo docker service ls --filter name=^${service_name}$ --format "{{.Name}}" | grep -q "^${service_name}$"; do
            echo "Waiting '$service_name' all removing..."
            sleep 1
        done

        echo "Service '$service_name' removed."
    else
        echo "Service removed failed"
    fi
else
    echo "Service '$service_name' not found."
fi

echo "Restart service '$service_name'."

# next command
cd /home/pi/apps/scripts/
./start-service-1.sh

cd /home/pi/apps
