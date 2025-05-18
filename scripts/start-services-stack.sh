#!/bin/bash

# 启动 Docker Stack
sudo docker stack deploy -c ~/apps/docker/configures/docker-compose.yml node-services-stack

# 获取所有服务的名称
services=$(sudo docker stack services node-services-stack --format "{{.Name}}")

# 死循环，直到所有服务都运行
while true; do
    all_running=true
    echo "============================="
    echo "Checking all services..."

    # 检查每个服务的状态
    for service in $services; do
        echo -n "Service: $service ->"

        # 获取该服务的状态
        replicas=$(sudo docker service ls --filter name=$service --format "{{.Replicas}}")

        if [[ $replicas != */* ]]; then
            echo "! Replica format invalid"
            all_running=false
            continue
        fi

        desired=$(echo $replicas | awk -F'/' '{print $2}' | awk '{print $1}')
        actual=$(echo $replicas | awk -F'/' '{print $1}' | awk '{print $1}')

        if [ "$actual" != "$desired" ]; then
            echo "Service $service is NOT ready (replicas: $actual/$desired)"
            all_running=false
        else
            echo "Ready ($actual/$desired)"
        fi
    done

    if [ "$all_running" = true ]; then
        echo  "All Service are running!"
        break
    fi

    # 等待一段时间后再次检查
    echo "Waiting for services to reach 'Running' state..."
    sleep 2
done
