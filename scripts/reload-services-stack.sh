#!/bin/bash

cd /home/pi/apps

stack_name="node-services-stack"

# 定义颜色
RED='\033[0;31m'       # 红色
GREEN='\033[0;32m'     # 绿色
YELLOW='\033[0;33m'    # 黄色
BLUE='\033[0;34m'      # 蓝色
CYAN='\033[0;36m'      # 青色
NC='\033[0m'          # 颜色重置（No Color）

# 输出带颜色的消息
echo -e "${GREEN}Stopping stack ${stack_name}...${NC}"
./scripts/stop-services-stack.sh
echo -e "${GREEN}Stopped stack ${stack_name}.${NC}"

echo -e "${GREEN}Restarting stack ${stack_name}...${NC}"
./scripts/start-services-stack.sh
echo -e "${GREEN}Started stack ${stack_name}!${NC}"
