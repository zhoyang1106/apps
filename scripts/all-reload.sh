cd /home/pi/apps/scripts

./update-docker-image.sh && \
./restart-redis-container.sh && \
./reload-services-stack.sh

sudo systemctl restart manager-server.service
echo $(sudo systemctl status manager-server.service)

echo "q"

cd /home/pi/apps
