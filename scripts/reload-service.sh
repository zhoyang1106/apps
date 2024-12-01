#!/bin/bash

service_name="node-service"

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
    echo "Remove service '$service_name' failed. Next step start."
fi

# next command
cd /home/pi/apps/scripts/
./start-service.sh

cd /home/pi/apps
