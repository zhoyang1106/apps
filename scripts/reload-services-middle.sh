GREEN='\033[0;32m'     # 绿色
NC='\033[0m'          # 颜色重置（No Color）

echo "restart services"

cd /home/pi/apps/scripts/

echo "restart redis container..."
./restart-redis-container.sh

echo "restart stack services..."
./reload-services-stack.sh

echo "restart manager-server.service..."
sudo systemctl restart manager-server.service

echo -e "${GREEN}all services restarted${NC}"
