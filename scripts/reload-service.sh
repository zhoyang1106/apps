#!/bin/bash

service_name="node-service"

# try remove service
if sudo docker service rm "$service_name"; then
    echo "服务 '$service_name' 移除成功。正在等待服务完全移除..."

    # check service
    while sudo docker service ls --filter name=^${service_name}$ --format "{{.Name}}" | grep -q "^${service_name}$"; do
        echo "等待服务 '$service_name' 完全移除中..."
        sleep 1
    done

    echo "服务 '$service_name' 已完全移除。"
else
    echo "移除服务 '$service_name' 失败或服务不存在。直接执行下一步。"
fi

# next command
cd /home/pi/apps/scripts/
./start-service.sh

cd /home/pi/apps
