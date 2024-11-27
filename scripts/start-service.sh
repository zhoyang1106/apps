service_name="node-service"
image="soar009/node-service-image"
tag="v5"
replicas=$(sudo docker node ls --filter "role=worker" | grep -c "Ready")
cpu_limit=0.8
network="node-service-net"

echo "Service Name: $service_name"
echo "Image: $image:$tag"
echo "Replicas: $replicas"
echo "CPU Limit: $cpu_limit"
echo "Network: $network"

sudo docker service create --name $service_name \
    --publish published=8080,target=8080 \
    --replicas $replicas \
    --limit-cpu $cpu_limit \
    --replicas-max-per-node 1 \
    --constraint "node.role==worker" \
    --network $network \
    $image:$tag