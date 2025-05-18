#!/bin/bash

# 文件路径
COMPOSE_FILE="/home/pi/apps/docker/configures/v6-config.yaml"

# 部署
sudo docker stack deploy -c $COMPOSE_FILE node-service-stack-v6

