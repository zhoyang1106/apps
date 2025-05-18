#!/bin/bash

# 清空旧的 env 文件
rm -rf /home/pi/apps/docker/configures/env_*.list

node_index=1
for node_id in $(sudo docker node ls --filter "role=worker" --format '{{.ID}}'); do
    worker_id=$(sudo docker node inspect --format '{{ index .Spec.Labels "worker_id" }}' "$node_id")
    hostname=$(sudo docker node inspect --format '{{.Description.Hostname}}' "$node_id")

    if [ -z "$worker_id" ]; then
        echo "Skipping node $node_id (no worker_id set)"
        continue
    fi

    # 为每个 worker 生成单独的 env 文件
    echo "WORKER_ID=$worker_id" > "/home/pi/apps/docker/configures/env_${worker_id}.list"
    
    echo "Generated env_${worker_id}.list for $hostname ($worker_id)"
    node_index=$((node_index + 1))
done

ls -l /home/pi/apps/docker/configures/
